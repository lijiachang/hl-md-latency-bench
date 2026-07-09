mod clock;
mod feed;
mod stats;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use clap::Parser;

use crate::clock::now_realtime_ns;
use crate::feed::{Event, FeedConfig};

const OFFICIAL_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Compare HyperLiquid bbo market-data latency across four websocket feeds:
/// official, QuickNode (hypercore ws), and two self-hosted nodes (ob / obaws).
#[derive(Parser)]
struct Args {
    /// Coin to subscribe (Hyperliquid perp naming, e.g. ETH, BTC, HYPE)
    #[arg(long, default_value = "ETH")]
    coin: String,
    /// Measurement duration in seconds (Ctrl-C ends early and still reports)
    #[arg(long, default_value_t = 600)]
    duration_secs: u64,
    /// Warm-up seconds discarded from the start of the measurement
    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,
    /// Directory for the async tracing log
    #[arg(long, default_value = "logs")]
    log_dir: String,
    /// Directory for the generated markdown report
    #[arg(long, default_value = ".")]
    report_dir: String,
}

fn main() {
    let args = Args::parse();

    std::fs::create_dir_all(&args.log_dir).expect("create log dir");
    let appender = tracing_appender::rolling::never(&args.log_dir, "bench.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_ansi(false)
        .init();

    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = stop.clone();
        ctrlc::set_handler(move || {
            println!("\n[bench] Ctrl-C received, finishing up and writing the report...");
            stop.store(true, Ordering::Relaxed);
        })
        .expect("install Ctrl-C handler");
    }

    // (name, url or reason-unavailable)
    let feeds: Vec<(&'static str, Result<String, String>)> = vec![
        ("official", Ok(OFFICIAL_WS_URL.to_string())),
        (
            "quicknode",
            std::env::var("QUICKNODE_WSS_URL")
                .map(|raw| quicknode_ws_url(&raw))
                .map_err(|_| "env QUICKNODE_WSS_URL not set".to_string()),
        ),
        ("ob", env_ws_url("OB_WSS_URL")),
        ("obaws", env_ws_url("OBAWS_WSS_URL")),
    ];
    let names: Vec<&'static str> = feeds.iter().map(|(n, _)| *n).collect();

    let start_ns = now_realtime_ns();
    let measure_start_ns = start_ns + args.warmup_secs * 1_000_000_000;

    let (tx, rx) = mpsc::channel::<Event>();
    let aggregator = {
        let names = names.clone();
        std::thread::Builder::new()
            .name("aggregator".into())
            .spawn(move || stats::run_aggregator(rx, names, measure_start_ns))
            .expect("spawn aggregator")
    };

    let mut workers = Vec::new();
    for (idx, (name, url)) in feeds.into_iter().enumerate() {
        match url {
            Ok(url) => {
                println!("[bench] {name}: connecting");
                tracing::info!(feed = name, "starting feed");
                let cfg = FeedConfig { name, url };
                let coin = args.coin.clone();
                let tx = tx.clone();
                let stop = stop.clone();
                let handle = std::thread::Builder::new()
                    .name(format!("feed-{name}"))
                    .spawn(move || feed::run_feed(idx, cfg, coin, tx, stop))
                    .expect("spawn feed thread");
                workers.push(handle);
            }
            Err(reason) => {
                tx.send(Event::Unavailable { feed: idx, reason })
                    .expect("aggregator alive");
            }
        }
    }
    drop(tx);

    println!(
        "[bench] coin={} duration={}s warmup={}s — Ctrl-C 可提前结束并输出报告",
        args.coin, args.duration_secs, args.warmup_secs
    );

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(200));
    }
    stop.store(true, Ordering::Relaxed);

    for handle in workers {
        let _ = handle.join();
    }
    let stats = aggregator.join().expect("aggregator panicked");

    let measured_secs = ((now_realtime_ns() - measure_start_ns) as f64 / 1e9).max(0.0);
    let report = stats::render_report(&stats, &args.coin, measured_secs, args.warmup_secs);

    println!("\n{report}");

    std::fs::create_dir_all(&args.report_dir).expect("create report dir");
    let file_name = format!("report_{}.md", chrono::Local::now().format("%Y%m%d_%H%M%S"));
    let path = std::path::Path::new(&args.report_dir).join(file_name);
    std::fs::write(&path, &report).expect("write report file");
    println!("[bench] 报告已写入 {}", path.display());
}

/// Normalize a QuickNode endpoint URL (`wss://host/<token>/`) to the
/// HyperCore websocket path used by the official QuickNode SDK:
/// `wss://host/<token>/hypercore/ws`.
fn quicknode_ws_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.ends_with("/hypercore/ws") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/hypercore/ws")
    }
}

fn env_ws_url(name: &str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|url| !url.is_empty())
        .ok_or_else(|| format!("env {name} not set"))
}

#[cfg(test)]
mod tests {
    use super::{env_ws_url, quicknode_ws_url};

    #[test]
    fn self_hosted_url_comes_from_env() {
        let key = "HL_MD_LATENCY_BENCH_TEST_OB_URL";
        std::env::remove_var(key);
        assert_eq!(env_ws_url(key), Err(format!("env {key} not set")));

        std::env::set_var(key, "  wss://node.example/ws?token=secret  ");
        assert_eq!(
            env_ws_url(key),
            Ok("wss://node.example/ws?token=secret".to_string())
        );

        std::env::set_var(key, "   ");
        assert_eq!(env_ws_url(key), Err(format!("env {key} not set")));
        std::env::remove_var(key);
    }

    #[test]
    fn quicknode_url_normalization() {
        assert_eq!(
            quicknode_ws_url("wss://sleek-light-star.quiknode.pro/abc123/"),
            "wss://sleek-light-star.quiknode.pro/abc123/hypercore/ws"
        );
        assert_eq!(
            quicknode_ws_url("wss://x.quiknode.pro/t/hypercore/ws"),
            "wss://x.quiknode.pro/t/hypercore/ws"
        );
    }
}
