mod clock;
mod feed;
mod grpc_feed;
mod stats;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use clap::Parser;

use crate::clock::now_realtime_ns;
use crate::feed::{Event, FeedConfig};
use crate::grpc_feed::GrpcFeedConfig;

const OFFICIAL_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Compare HyperLiquid bbo market-data latency across four feeds:
/// official ws, QuickNode gRPC StreamBboBook, and two self-hosted node ws
/// endpoints (ob / obaws).
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

    // (name, feed config or reason-unavailable)
    let feeds: Vec<(&'static str, Result<FeedKind, String>)> = vec![
        (
            "official",
            Ok(FeedKind::Ws(FeedConfig {
                name: "official",
                url: OFFICIAL_WS_URL.to_string(),
                x_token: None,
            })),
        ),
        ("quicknode", quicknode_grpc_config().map(FeedKind::Grpc)),
        (
            "ob",
            env_ws_url("OB_WSS_URL").map(|url| {
                FeedKind::Ws(FeedConfig {
                    name: "ob",
                    url,
                    x_token: None,
                })
            }),
        ),
        (
            "obaws",
            env_ws_url("OBAWS_WSS_URL").map(|url| {
                FeedKind::Ws(FeedConfig {
                    name: "obaws",
                    url,
                    x_token: None,
                })
            }),
        ),
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
    for (idx, (name, cfg)) in feeds.into_iter().enumerate() {
        match cfg {
            Ok(kind) => {
                println!("[bench] {name}: connecting");
                tracing::info!(feed = name, "starting feed");
                let coin = args.coin.clone();
                let tx = tx.clone();
                let stop = stop.clone();
                let handle = std::thread::Builder::new()
                    .name(format!("feed-{name}"))
                    .spawn(move || match kind {
                        FeedKind::Ws(cfg) => feed::run_feed(idx, cfg, coin, tx, stop),
                        FeedKind::Grpc(cfg) => grpc_feed::run_grpc_feed(idx, cfg, coin, tx, stop),
                    })
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

enum FeedKind {
    Ws(FeedConfig),
    Grpc(GrpcFeedConfig),
}

/// Build the QuickNode gRPC StreamBboBook config. QuickNode's bbo dataset is
/// gRPC-only (port 10000, `x-token` metadata):
/// <https://www.quicknode.com/docs/hyperliquid/datasets/bbo-book>
///
/// Endpoint candidates, in priority order:
/// 1. `QUICKNODE_GRPC_URL` as given (scheme normalized to https)
/// 2. from `QUICKNODE_RPC_URL` / `QUICKNODE_WSS_URL` host: `https://<host>:10000`,
///    plus the `<name>.hype-mainnet.quiknode.pro:10000` variant the QuickNode
///    docs use for HyperCore endpoints
///
/// Token candidates: `QUICKNODE_TOKEN` / `QUICKNODE_GRPC_TOKEN` /
/// `QUICKNODE_API_KEY` env vars, then the URL path token. The gRPC feed tries
/// every (endpoint, token) pair and pins the first that streams data.
fn quicknode_grpc_config() -> Result<GrpcFeedConfig, String> {
    let mut endpoints: Vec<String> = Vec::new();
    let mut tokens: Vec<String> = Vec::new();

    if let Some(raw) = env_nonempty("QUICKNODE_GRPC_URL") {
        endpoints.push(normalize_grpc_endpoint(&raw));
    }

    let base_url = env_nonempty("QUICKNODE_RPC_URL").or_else(|| env_nonempty("QUICKNODE_WSS_URL"));
    if let Some(raw) = &base_url {
        if let Ok(url) = url::Url::parse(raw) {
            if let Some(host) = url.host_str() {
                push_unique(&mut endpoints, format!("https://{host}:10000"));
                if let Some(name) = host.strip_suffix(".quiknode.pro") {
                    if !name.contains('.') {
                        push_unique(
                            &mut endpoints,
                            format!("https://{name}.hype-mainnet.quiknode.pro:10000"),
                        );
                    }
                }
            }
            if let Some(token) = url
                .path_segments()
                .and_then(|mut segs| segs.find(|s| !s.is_empty()))
            {
                push_unique(&mut tokens, token.to_string());
            }
        }
    }

    for name in [
        "QUICKNODE_TOKEN",
        "QUICKNODE_GRPC_TOKEN",
        "QUICKNODE_API_KEY",
    ] {
        if let Some(token) = env_nonempty(name) {
            push_unique(&mut tokens, token);
        }
    }
    if endpoints.is_empty() {
        return Err(
            "no QuickNode gRPC endpoint (set QUICKNODE_GRPC_URL or QUICKNODE_RPC_URL)".to_string(),
        );
    }
    if tokens.is_empty() {
        return Err(
            "no QuickNode token (set QUICKNODE_TOKEN or use a URL with a token path)".to_string(),
        );
    }

    Ok(GrpcFeedConfig {
        name: "quicknode",
        endpoints,
        tokens,
    })
}

fn normalize_grpc_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn push_unique(list: &mut Vec<String>, value: String) {
    if !list.contains(&value) {
        list.push(value);
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
    use super::{env_ws_url, normalize_grpc_endpoint, quicknode_grpc_config};

    /// Snapshot-and-restore guard so env-var tests don't leak between tests
    /// (cargo runs them in threads of one process).
    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn clear(keys: &[&'static str]) -> Self {
            let saved = keys.iter().map(|k| (*k, std::env::var(k).ok())).collect();
            for k in keys {
                std::env::remove_var(k);
            }
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.saved {
                match v {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    const QN_KEYS: &[&str] = &[
        "QUICKNODE_GRPC_URL",
        "QUICKNODE_RPC_URL",
        "QUICKNODE_WSS_URL",
        "QUICKNODE_TOKEN",
        "QUICKNODE_GRPC_TOKEN",
        "QUICKNODE_API_KEY",
    ];

    // Env-var manipulation is process-global; keep everything that touches
    // QUICKNODE_* in ONE test to avoid cross-test races.
    #[test]
    fn quicknode_grpc_config_derivation() {
        let _guard = EnvGuard::clear(QN_KEYS);

        // nothing set → unavailable with actionable reason
        assert!(quicknode_grpc_config()
            .unwrap_err()
            .contains("QUICKNODE_GRPC_URL"));

        // RPC URL alone provides both endpoint candidates and the path token
        std::env::set_var(
            "QUICKNODE_RPC_URL",
            "https://sleek-light-star.quiknode.pro/abc123/",
        );
        let cfg = quicknode_grpc_config().unwrap();
        assert_eq!(
            cfg.endpoints,
            vec![
                "https://sleek-light-star.quiknode.pro:10000".to_string(),
                "https://sleek-light-star.hype-mainnet.quiknode.pro:10000".to_string(),
            ]
        );
        assert_eq!(cfg.tokens, vec!["abc123".to_string()]);

        // explicit gRPC URL takes priority; env tokens appended after path token
        std::env::set_var(
            "QUICKNODE_GRPC_URL",
            "my-ep.hype-mainnet.quiknode.pro:10000",
        );
        std::env::set_var("QUICKNODE_TOKEN", " tok-env ");
        let cfg = quicknode_grpc_config().unwrap();
        assert_eq!(
            cfg.endpoints[0],
            "https://my-ep.hype-mainnet.quiknode.pro:10000"
        );
        assert_eq!(
            cfg.tokens,
            vec!["abc123".to_string(), "tok-env".to_string()]
        );
    }

    #[test]
    fn grpc_endpoint_normalization() {
        assert_eq!(
            normalize_grpc_endpoint("host.quiknode.pro:10000"),
            "https://host.quiknode.pro:10000"
        );
        assert_eq!(
            normalize_grpc_endpoint("https://host.quiknode.pro:10000/"),
            "https://host.quiknode.pro:10000"
        );
    }

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
}
