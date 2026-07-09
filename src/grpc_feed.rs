//! QuickNode gRPC StreamBboBook feed. QuickNode's bbo dataset is only
//! available over gRPC (port 10000, `x-token` metadata auth) — see
//! <https://www.quicknode.com/docs/hyperliquid/datasets/bbo-book>.
//!
//! Retry semantics mirror the websocket feed in `feed.rs`: a few initial
//! attempts before the feed is declared unavailable, then reconnect with
//! exponential backoff once it has streamed at least one update. Because the
//! exact endpoint host / token layout varies per QuickNode plan, the initial
//! round tries every (endpoint, token) candidate pair and pins the first pair
//! that actually streams data.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic::Request;

use crate::clock::now_realtime_ns;
use crate::feed::Event;

pub mod pb {
    tonic::include_proto!("hyperliquid");
}

use pb::order_book_streaming_client::OrderBookStreamingClient;
use pb::{BboBookRequest, BboBookUpdate};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TICK: Duration = Duration::from_secs(1);
const FIRST_MESSAGE_TIMEOUT: Duration = Duration::from_secs(15);
const INITIAL_CONNECT_ROUNDS: u32 = 3;
const MAX_BACKOFF_SECS: u64 = 30;

#[derive(Debug)]
pub struct GrpcFeedConfig {
    pub name: &'static str,
    /// Candidate gRPC endpoints, e.g. `https://host:10000`
    pub endpoints: Vec<String>,
    /// Candidate `x-token` values
    pub tokens: Vec<String>,
}

enum SessionEnd {
    Stopped,
    Lost(String),
}

pub fn run_grpc_feed(
    feed: usize,
    cfg: GrpcFeedConfig,
    coin: String,
    tx: Sender<Event>,
    stop: Arc<AtomicBool>,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime for gRPC feed");
    rt.block_on(run(feed, cfg, coin, tx, stop));
}

async fn run(
    feed: usize,
    cfg: GrpcFeedConfig,
    coin: String,
    tx: Sender<Event>,
    stop: Arc<AtomicBool>,
) {
    let mut working: Option<(String, String)> = None;
    let mut initial_rounds = 0u32;
    let mut backoff_secs = 1u64;

    while !stop.load(Ordering::Relaxed) {
        let outcome = if let Some((endpoint, token)) = &working {
            session(feed, cfg.name, endpoint, token, &coin, &tx, &stop).await
        } else {
            // Initial discovery: try every candidate pair until one streams.
            let mut last_err = "no endpoint/token candidates".to_string();
            let mut found = Err(String::new());
            'candidates: for endpoint in &cfg.endpoints {
                for token in &cfg.tokens {
                    match session(feed, cfg.name, endpoint, token, &coin, &tx, &stop).await {
                        Err(reason) => {
                            tracing::warn!(
                                feed = cfg.name,
                                endpoint,
                                reason,
                                "gRPC candidate failed"
                            );
                            last_err = format!("{endpoint}: {reason}");
                        }
                        ok => {
                            working = Some((endpoint.clone(), token.clone()));
                            found = ok;
                            break 'candidates;
                        }
                    }
                }
            }
            if working.is_none() {
                found = Err(last_err);
            }
            found
        };

        match outcome {
            Ok(SessionEnd::Stopped) => return,
            Ok(SessionEnd::Lost(reason)) => {
                backoff_secs = 1;
                tracing::warn!(feed = cfg.name, reason, "gRPC stream lost, reconnecting");
                if tx.send(Event::Disconnect { feed }).is_err() {
                    return;
                }
                sleep_interruptible(Duration::from_secs(backoff_secs), &stop).await;
            }
            Err(reason) => {
                if working.is_some() {
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    tracing::warn!(
                        feed = cfg.name,
                        reason,
                        backoff_secs,
                        "gRPC reconnect attempt failed"
                    );
                    sleep_interruptible(Duration::from_secs(backoff_secs), &stop).await;
                } else {
                    initial_rounds += 1;
                    tracing::warn!(
                        feed = cfg.name,
                        reason,
                        round = initial_rounds,
                        "gRPC initial connect round failed"
                    );
                    if initial_rounds >= INITIAL_CONNECT_ROUNDS {
                        let _ = tx.send(Event::Unavailable { feed, reason });
                        return;
                    }
                    sleep_interruptible(Duration::from_secs(2), &stop).await;
                }
            }
        }
    }
}

/// One streaming session. `Err` means it died before the first update.
async fn session(
    feed: usize,
    name: &'static str,
    endpoint: &str,
    token: &str,
    coin: &str,
    tx: &Sender<Event>,
    stop: &Arc<AtomicBool>,
) -> Result<SessionEnd, String> {
    let channel = Endpoint::from_shared(endpoint.to_string())
        .map_err(|e| format!("bad endpoint: {e}"))?
        .tls_config(ClientTlsConfig::new())
        .map_err(|e| format!("tls config failed: {}", format_error_chain(&e)))?
        .tcp_nodelay(true)
        .connect_timeout(CONNECT_TIMEOUT)
        .connect()
        .await
        .map_err(|e| format!("connect failed: {}", format_error_chain(&e)))?;

    let mut client =
        OrderBookStreamingClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let token_meta: MetadataValue<_> = token
        .parse()
        .map_err(|e| format!("bad x-token metadata: {e}"))?;
    let mut request = Request::new(BboBookRequest {
        coins: vec![coin.to_string()],
    });
    request.metadata_mut().insert("x-token", token_meta);

    let mut stream = client
        .stream_bbo_book(request)
        .await
        .map_err(|e| format!("stream start failed: {}", format_error_chain(&e)))?
        .into_inner();
    tracing::info!(
        feed = name,
        endpoint,
        "gRPC connected, StreamBboBook started"
    );

    let started = Instant::now();
    let mut subscribed = false;

    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(SessionEnd::Stopped);
        }
        match tokio::time::timeout(READ_TICK, stream.message()).await {
            Err(_elapsed) => {
                if !subscribed && started.elapsed() > FIRST_MESSAGE_TIMEOUT {
                    return Err("no bbo data within 15s".to_string());
                }
            }
            Ok(Ok(Some(update))) => {
                let local_ns = now_realtime_ns();
                if !subscribed {
                    subscribed = true;
                    let _ = tx.send(Event::Subscribed { feed });
                }
                let time_ms = update.time;
                let text = bbo_update_json(&update);
                tracing::info!(target: "raw_msg", feed = name, local_ns, "raw grpc message: {}", text);
                let latency_ns = local_ns as i64 - time_ms as i64 * 1_000_000;
                tracing::info!(target: "sample", feed = name, time_ms, local_ns, latency_ns);
                if tx
                    .send(Event::Sample {
                        feed,
                        time_ms,
                        local_ns,
                        text,
                    })
                    .is_err()
                {
                    return Ok(SessionEnd::Stopped);
                }
            }
            Ok(Ok(None)) => {
                return if subscribed {
                    Ok(SessionEnd::Lost("stream ended by server".to_string()))
                } else {
                    Err("stream ended before any data".to_string())
                };
            }
            Ok(Err(status)) => {
                let reason = format!("stream error: {}", format_error_chain(&status));
                return if subscribed {
                    Ok(SessionEnd::Lost(reason))
                } else {
                    Err(reason)
                };
            }
        }
    }
}

fn format_error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut msg = err.to_string();
    let mut source = err.source();
    while let Some(err) = source {
        let next = err.to_string();
        if !next.is_empty() && !msg.contains(&next) {
            msg.push_str(": ");
            msg.push_str(&next);
        }
        source = err.source();
    }
    msg
}

/// Render a BboBookUpdate as the self-hosted-node JSON shape so the
/// aggregator's `parse_bbo_content` treats ws and gRPC samples uniformly.
fn bbo_update_json(u: &BboBookUpdate) -> String {
    let mut s = format!(
        r#"{{"channel":"bbo","data":{{"coin":"{}","time":{}"#,
        u.coin, u.time
    );
    if let Some(b) = &u.bid {
        s.push_str(&format!(
            r#","bid":{{"px":"{}","sz":"{}","n":{}}}"#,
            b.px, b.sz, b.n
        ));
    }
    if let Some(a) = &u.ask {
        s.push_str(&format!(
            r#","ask":{{"px":"{}","sz":"{}","n":{}}}"#,
            a.px, a.sz, a.n
        ));
    }
    s.push_str("}}");
    s
}

async fn sleep_interruptible(dur: Duration, stop: &Arc<AtomicBool>) {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::bbo_update_json;
    use super::pb::{BboBookUpdate, L2Level};
    use crate::feed::extract_time_ms;
    use crate::stats::parse_bbo_content;

    #[test]
    fn grpc_update_json_matches_ws_content_key() {
        let update = BboBookUpdate {
            coin: "ETH".to_string(),
            time: 1783602275832,
            block_number: 42,
            bid: Some(L2Level {
                px: "1745.8".to_string(),
                sz: "128.6371".to_string(),
                n: 18,
            }),
            ask: Some(L2Level {
                px: "1745.9".to_string(),
                sz: "64.2003".to_string(),
                n: 24,
            }),
        };
        let json = bbo_update_json(&update);
        assert_eq!(extract_time_ms(&json), Some(1783602275832));

        let ws_msg = r#"{"channel":"bbo","data":{"coin":"ETH","time":1783602275832,"bid":{"px":"1745.8","sz":"128.6371","n":18},"ask":{"px":"1745.9","sz":"64.2003","n":24}}}"#;
        assert_eq!(parse_bbo_content(&json), parse_bbo_content(ws_msg));
        assert!(parse_bbo_content(&json).is_some());
    }

    #[test]
    fn one_sided_update_renders_without_missing_side() {
        let update = BboBookUpdate {
            coin: "X".to_string(),
            time: 5,
            block_number: 1,
            bid: None,
            ask: Some(L2Level {
                px: "2".to_string(),
                sz: "3".to_string(),
                n: 1,
            }),
        };
        let key = parse_bbo_content(&bbo_update_json(&update)).unwrap();
        assert!(key.0.is_none());
        assert!(key.1.is_some());
    }
}
