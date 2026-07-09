use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::{HandshakeError, HandshakeRole};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};
use url::Url;

use crate::clock::now_realtime_ns;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TICK: Duration = Duration::from_secs(1);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const INITIAL_CONNECT_ATTEMPTS: u32 = 3;
const MAX_BACKOFF_SECS: u64 = 30;

const PING_MSG: &str = r#"{"method":"ping"}"#;

#[derive(Debug)]
pub enum Event {
    Subscribed {
        feed: usize,
    },
    Sample {
        feed: usize,
        time_ms: u64,
        local_ns: u64,
        /// raw bbo message text; content normalization happens in the
        /// aggregator thread to keep the receive hot path minimal
        text: String,
    },
    Disconnect {
        feed: usize,
    },
    Unavailable {
        feed: usize,
        reason: String,
    },
}

pub struct FeedConfig {
    pub name: &'static str,
    pub url: String,
    pub x_token: Option<String>,
}

enum SessionEnd {
    Stopped,
    Lost(String),
}

/// Run one feed until `stop` is set. Identical code path for every feed to keep
/// the comparison fair: timestamp immediately after the socket read, byte-scan
/// extraction of the `time` field, no serde in the hot loop.
pub fn run_feed(
    feed: usize,
    cfg: FeedConfig,
    coin: String,
    tx: Sender<Event>,
    stop: Arc<AtomicBool>,
) {
    let sub_msg =
        format!(r#"{{"method":"subscribe","subscription":{{"type":"bbo","coin":"{coin}"}}}}"#);
    let mut connected_once = false;
    let mut initial_failures = 0u32;
    let mut backoff_secs = 1u64;

    while !stop.load(Ordering::Relaxed) {
        match session(feed, &cfg, &sub_msg, &tx, &stop) {
            Ok(SessionEnd::Stopped) => return,
            Ok(SessionEnd::Lost(reason)) => {
                connected_once = true;
                backoff_secs = 1;
                tracing::warn!(feed = cfg.name, reason, "connection lost, reconnecting");
                if tx.send(Event::Disconnect { feed }).is_err() {
                    return;
                }
                sleep_interruptible(Duration::from_secs(backoff_secs), &stop);
            }
            Err(reason) => {
                if connected_once {
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    tracing::warn!(
                        feed = cfg.name,
                        reason,
                        backoff_secs,
                        "reconnect attempt failed"
                    );
                    sleep_interruptible(Duration::from_secs(backoff_secs), &stop);
                } else {
                    initial_failures += 1;
                    tracing::warn!(
                        feed = cfg.name,
                        reason,
                        attempt = initial_failures,
                        "initial connect failed"
                    );
                    if initial_failures >= INITIAL_CONNECT_ATTEMPTS {
                        let _ = tx.send(Event::Unavailable { feed, reason });
                        return;
                    }
                    sleep_interruptible(Duration::from_secs(2), &stop);
                }
            }
        }
    }
}

/// One websocket session: connect, subscribe, stream until stop/disconnect.
/// `Err` means the session died before the subscription was confirmed.
fn session(
    feed: usize,
    cfg: &FeedConfig,
    sub_msg: &str,
    tx: &Sender<Event>,
    stop: &Arc<AtomicBool>,
) -> Result<SessionEnd, String> {
    let mut ws = connect(&cfg.url, cfg.x_token.as_deref())?;
    ws.send(Message::Text(sub_msg.to_string()))
        .map_err(|e| format!("subscribe send failed: {e}"))?;
    tracing::info!(feed = cfg.name, "connected, subscription sent");

    let sub_sent_at = Instant::now();
    let mut subscribed = false;
    let mut last_ping = Instant::now();

    loop {
        if stop.load(Ordering::Relaxed) {
            let _ = ws.close(None);
            return Ok(SessionEnd::Stopped);
        }

        match ws.read() {
            Ok(msg) => {
                let local_ns = now_realtime_ns();
                log_raw_message(cfg.name, local_ns, &msg);
                if let Message::Text(txt) = msg {
                    if txt.contains(r#""channel":"bbo""#) {
                        if !subscribed {
                            subscribed = true;
                            let _ = tx.send(Event::Subscribed { feed });
                        }
                        if let Some(time_ms) = extract_time_ms(&txt) {
                            let latency_ns = local_ns as i64 - time_ms as i64 * 1_000_000;
                            tracing::info!(
                                target: "sample",
                                feed = cfg.name,
                                time_ms,
                                local_ns,
                                latency_ns
                            );
                            if tx
                                .send(Event::Sample {
                                    feed,
                                    time_ms,
                                    local_ns,
                                    text: txt,
                                })
                                .is_err()
                            {
                                let _ = ws.close(None);
                                return Ok(SessionEnd::Stopped);
                            }
                        } else {
                            tracing::warn!(feed = cfg.name, msg = %txt, "bbo message without parsable time");
                        }
                    } else if txt.contains(r#""channel":"subscriptionResponse""#) {
                        if !subscribed {
                            subscribed = true;
                            let _ = tx.send(Event::Subscribed { feed });
                        }
                        tracing::info!(feed = cfg.name, "subscription confirmed");
                    } else if txt.contains(r#""channel":"error""#) {
                        if !subscribed {
                            return Err(format!("subscription rejected: {txt}"));
                        }
                        tracing::warn!(feed = cfg.name, msg = %txt, "error message from feed");
                    }
                }
                // pong and anything else: ignore
            }
            Err(tungstenite::Error::Io(e))
                if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                // read-timeout tick; fall through to ping / deadline checks
            }
            Err(e) => {
                return if subscribed {
                    Ok(SessionEnd::Lost(e.to_string()))
                } else {
                    Err(e.to_string())
                };
            }
        }

        if !subscribed && sub_sent_at.elapsed() > SUBSCRIBE_TIMEOUT {
            return Err("no subscription confirmation or bbo data within 15s".to_string());
        }
        if last_ping.elapsed() >= PING_INTERVAL {
            ws.send(Message::Text(PING_MSG.to_string()))
                .map_err(|e| format!("ping send failed: {e}"))?;
            last_ping = Instant::now();
        }
    }
}

fn log_raw_message(feed_name: &'static str, local_ns: u64, msg: &Message) {
    match msg {
        Message::Text(txt) => tracing::info!(
            target: "raw_msg",
            feed = feed_name,
            local_ns,
            "raw websocket message: {}",
            txt
        ),
        _ => tracing::info!(
            target: "raw_msg",
            feed = feed_name,
            local_ns,
            raw = ?msg,
            "raw websocket message"
        ),
    }
}

/// TCP connect with timeout, then TLS + websocket handshake. A plain
/// `tungstenite::connect` has no connect timeout, which would stall for
/// minutes on an unreachable self-hosted node.
fn connect(
    url_str: &str,
    x_token: Option<&str>,
) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, String> {
    let url = Url::parse(url_str).map_err(|e| format!("bad url: {e}"))?;
    let host = url.host_str().ok_or("url has no host")?.to_string();
    let port = url.port_or_known_default().unwrap_or(443);

    let addrs: Vec<_> = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|e| format!("dns resolution failed: {e}"))?
        .collect();
    let mut last_err = "no address resolved".to_string();
    let mut stream = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(e) => last_err = format!("tcp connect to {addr} failed: {e}"),
        }
    }
    let stream = stream.ok_or(last_err)?;

    stream.set_nodelay(true).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(HANDSHAKE_READ_TIMEOUT))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(HANDSHAKE_READ_TIMEOUT))
        .map_err(|e| e.to_string())?;

    let mut request = url_str
        .into_client_request()
        .map_err(|e| format!("bad websocket request: {e}"))?;
    if let Some(token) = x_token {
        request.headers_mut().insert(
            "x-token",
            token
                .parse()
                .map_err(|e| format!("bad x-token header: {e}"))?,
        );
    }

    let (ws, _resp) = tungstenite::client_tls(request, stream)
        .map_err(|e| format!("ws handshake failed: {}", format_handshake_error(e)))?;

    // Short read timeout so the loop can tick for pings and stop checks.
    if let Some(tcp) = tcp_stream(&ws) {
        tcp.set_read_timeout(Some(READ_TICK))
            .map_err(|e| e.to_string())?;
    }
    Ok(ws)
}

fn format_ws_error(err: &tungstenite::Error) -> String {
    let tungstenite::Error::Http(resp) = err else {
        return err.to_string();
    };
    let mut msg = format!("HTTP error: {}", resp.status());
    if let Some(body) = resp.body().as_ref().filter(|body| !body.is_empty()) {
        let body = String::from_utf8_lossy(body);
        let body = body.trim();
        if !body.is_empty() {
            msg.push_str(": ");
            msg.push_str(body);
        }
    }
    msg
}

fn format_handshake_error<S: HandshakeRole>(err: HandshakeError<S>) -> String {
    match err {
        HandshakeError::Failure(err) => format_ws_error(&err),
        other => other.to_string(),
    }
}

fn tcp_stream(ws: &WebSocket<MaybeTlsStream<TcpStream>>) -> Option<&TcpStream> {
    match ws.get_ref() {
        MaybeTlsStream::Plain(s) => Some(s),
        MaybeTlsStream::Rustls(s) => Some(s.get_ref()),
        _ => None,
    }
}

fn sleep_interruptible(dur: Duration, stop: &Arc<AtomicBool>) {
    let deadline = Instant::now() + dur;
    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Extract the first `"time":<digits>` value. Both known bbo payload shapes
/// (official `"bbo":[..]` array, self-hosted `"bid"/"ask"` objects) carry
/// `time` directly after `coin` inside `data`, before any nested objects.
pub fn extract_time_ms(msg: &str) -> Option<u64> {
    const KEY: &[u8] = b"\"time\":";
    let bytes = msg.as_bytes();
    let pos = bytes.windows(KEY.len()).position(|w| w == KEY)?;
    let mut i = pos + KEY.len();
    while i < bytes.len() && bytes[i] == b' ' {
        i += 1;
    }
    let start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == start {
        return None;
    }
    msg[start..i].parse().ok()
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use tungstenite::Message;

    use super::{extract_time_ms, format_ws_error, log_raw_message};

    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn raw_websocket_message_log_contains_payload() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer_buf = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || SharedWriter(writer_buf.clone()))
            .with_ansi(false)
            .with_target(false)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_raw_message(
                "official",
                123,
                &Message::Text(r#"{"channel":"pong"}"#.to_string()),
            );
        });

        let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(output.contains("raw websocket message"));
        assert!(output.contains("feed=\"official\""));
        assert!(output.contains("local_ns=123"));
        assert!(output.contains(r#"{"channel":"pong"}"#));
    }

    #[test]
    fn extracts_time_from_official_format() {
        let msg = r#"{"channel":"bbo","data":{"coin":"HYPE","time":1763470097148,"bbo":[{"px":"41.485","sz":"70.0","n":1},{"px":"41.487","sz":"25.25","n":1}]}}"#;
        assert_eq!(extract_time_ms(msg), Some(1763470097148));
    }

    #[test]
    fn extracts_time_from_node_format() {
        let msg = r#"{"channel":"bbo","data":{"coin":"ETH","time":1783602275832,"bid":{"px":"1745.8","sz":"128.6371","n":18},"ask":{"px":"1745.9","sz":"64.2003","n":24}}}"#;
        assert_eq!(extract_time_ms(msg), Some(1783602275832));
    }

    #[test]
    fn missing_time_returns_none() {
        assert_eq!(extract_time_ms(r#"{"channel":"pong"}"#), None);
        assert_eq!(extract_time_ms(r#"{"data":{"time":}}"#), None);
    }

    #[test]
    fn websocket_http_error_includes_response_body() {
        let mut response = tungstenite::handshake::client::Response::new(Some(
            br#"{"error":"Network mismatch. Consider adding the ChainPrism add-on to your endpoint."}"#
                .to_vec(),
        ));
        *response.status_mut() = tungstenite::http::StatusCode::UNAUTHORIZED;
        let err = tungstenite::Error::Http(response);

        assert!(format_ws_error(&err).contains("401 Unauthorized"));
        assert!(format_ws_error(&err).contains("Network mismatch"));
    }
}
