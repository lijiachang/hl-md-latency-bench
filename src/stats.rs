use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::feed::Event;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// One side of the book, normalized for cross-feed content comparison.
/// px/sz are stored as f64 bit patterns so `"70.0"` and `"70"` compare equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SideKey {
    px_bits: u64,
    sz_bits: u64,
    n: u32,
}

/// Canonical bbo content: (bid, ask); `None` = side absent/null.
/// Covers both wire formats — official `"bbo":[bid,ask]` and
/// self-hosted `"bid":{…},"ask":{…}`.
pub type ContentKey = (Option<SideKey>, Option<SideKey>);

fn side_key(v: &serde_json::Value) -> Option<SideKey> {
    let px: f64 = v.get("px")?.as_str()?.parse().ok()?;
    let sz: f64 = v.get("sz")?.as_str()?.parse().ok()?;
    let n = v.get("n")?.as_u64()? as u32;
    Some(SideKey {
        px_bits: px.to_bits(),
        sz_bits: sz.to_bits(),
        n,
    })
}

/// Normalize a bbo message's content. `None` only when the message cannot be
/// parsed at all; an empty side maps to `None` inside the key.
pub fn parse_bbo_content(text: &str) -> Option<ContentKey> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    let data = v.get("data")?;
    if let Some(arr) = data.get("bbo").and_then(|b| b.as_array()) {
        Some((
            arr.first().and_then(side_key),
            arr.get(1).and_then(side_key),
        ))
    } else if data.get("bid").is_some() || data.get("ask").is_some() {
        Some((
            data.get("bid").and_then(side_key),
            data.get("ask").and_then(side_key),
        ))
    } else {
        None
    }
}

pub struct FeedStats {
    pub name: &'static str,
    pub subscribed: bool,
    pub unavailable: Option<String>,
    pub raw_msgs: u64,
    pub dup_msgs: u64,
    pub warmup_dropped: u64,
    pub unparsed_content: u64,
    pub disconnects: u32,
    /// latency (local receive − exchange time), ns, one entry per unique `time_ms`
    pub latencies_ns: Vec<i64>,
    /// time_ms → first-arrival local timestamp (ns); report 1 granularity
    pub first_arrival: HashMap<u64, u64>,
    /// (time_ms, normalized bbo content) → first-arrival local ns; report 2
    /// granularity: only messages with identical content count as "the same
    /// update", so a feed pushing many intra-block states gains no edge from
    /// updates other feeds never emit
    pub content_arrival: HashMap<(u64, ContentKey), u64>,
}

impl FeedStats {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            subscribed: false,
            unavailable: None,
            raw_msgs: 0,
            dup_msgs: 0,
            warmup_dropped: 0,
            unparsed_content: 0,
            disconnects: 0,
            latencies_ns: Vec::new(),
            first_arrival: HashMap::new(),
            content_arrival: HashMap::new(),
        }
    }

    pub fn available(&self) -> bool {
        self.unavailable.is_none() && !self.first_arrival.is_empty()
    }
}

/// Collect events from all feed threads until every sender is dropped.
/// Samples arriving before `measure_start_ns` (warm-up) are counted but not kept.
pub fn run_aggregator(
    rx: Receiver<Event>,
    names: Vec<&'static str>,
    measure_start_ns: u64,
) -> Vec<FeedStats> {
    let mut stats: Vec<FeedStats> = names.into_iter().map(FeedStats::new).collect();
    let started = Instant::now();
    let mut last_heartbeat = Instant::now();

    loop {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Event::Sample {
                feed,
                time_ms,
                local_ns,
                text,
            }) => {
                let s = &mut stats[feed];
                s.raw_msgs += 1;
                if local_ns < measure_start_ns {
                    s.warmup_dropped += 1;
                    continue;
                }
                // report 1: first arrival per block time
                if let std::collections::hash_map::Entry::Vacant(e) = s.first_arrival.entry(time_ms)
                {
                    e.insert(local_ns);
                    s.latencies_ns
                        .push(local_ns as i64 - time_ms as i64 * 1_000_000);
                } else {
                    s.dup_msgs += 1;
                }
                // report 2: first arrival per (time, exact bbo content)
                if let Some(key) = parse_bbo_content(&text) {
                    s.content_arrival.entry((time_ms, key)).or_insert(local_ns);
                } else {
                    s.unparsed_content += 1;
                }
            }
            Ok(Event::Subscribed { feed }) => stats[feed].subscribed = true,
            Ok(Event::Disconnect { feed }) => stats[feed].disconnects += 1,
            Ok(Event::Unavailable { feed, reason }) => {
                println!("[bench] feed {} unavailable: {}", stats[feed].name, reason);
                stats[feed].unavailable = Some(reason);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        if last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
            last_heartbeat = Instant::now();
            let counts: Vec<String> = stats
                .iter()
                .map(|s| {
                    if s.unavailable.is_some() {
                        format!("{}=unavailable", s.name)
                    } else {
                        format!("{}={}", s.name, s.latencies_ns.len())
                    }
                })
                .collect();
            println!(
                "[bench] t+{}s samples: {}",
                started.elapsed().as_secs(),
                counts.join(" ")
            );
        }
    }
    stats
}

fn percentile_ns(sorted: &[i64], p: f64) -> i64 {
    debug_assert!(!sorted.is_empty());
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn ms(ns: i64) -> String {
    format!("{:.3}", ns as f64 / 1_000_000.0)
}

fn dist_cells(sorted: &[i64]) -> Vec<String> {
    vec![
        ms(sorted[0]),
        ms(percentile_ns(sorted, 25.0)),
        ms(percentile_ns(sorted, 50.0)),
        ms(percentile_ns(sorted, 75.0)),
        ms(percentile_ns(sorted, 99.0)),
        ms(sorted[sorted.len() - 1]),
    ]
}

fn display_width(s: &str) -> usize {
    s.chars().map(|c| if is_wide(c) { 2 } else { 1 }).sum()
}

fn is_wide(c: char) -> bool {
    matches!(
        c as u32,
        0x1100..=0x115f
            | 0x2e80..=0xa4cf
            | 0xac00..=0xd7a3
            | 0xf900..=0xfaff
            | 0xfe10..=0xfe6f
            | 0xff00..=0xffe6
    )
}

fn write_table(out: &mut String, headers: &[&str], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = headers.iter().map(|h| display_width(h)).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(display_width(cell));
        }
    }

    write_table_row(out, headers.iter().copied(), &widths);
    let separators: Vec<String> = widths.iter().map(|w| "-".repeat((*w).max(3))).collect();
    write_table_row(out, separators.iter().map(String::as_str), &widths);
    for row in rows {
        write_table_row(out, row.iter().map(String::as_str), &widths);
    }
}

fn write_table_row<'a>(out: &mut String, cells: impl Iterator<Item = &'a str>, widths: &[usize]) {
    let _ = write!(out, "|");
    for (cell, width) in cells.zip(widths) {
        let _ = write!(
            out,
            " {cell}{} |",
            " ".repeat(width.saturating_sub(display_width(cell)))
        );
    }
    let _ = writeln!(out);
}

pub fn render_report(
    stats: &[FeedStats],
    coin: &str,
    measured_secs: f64,
    warmup_secs: u64,
) -> String {
    let mut out = String::new();
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S %z");
    let _ = writeln!(out, "# HyperLiquid bbo 行情延迟对比报告\n");
    let _ = writeln!(out, "- 生成时间: {now}");
    let _ = writeln!(out, "- 币种: {coin}");
    let _ = writeln!(
        out,
        "- 有效测量时长: {measured_secs:.0}s (预热丢弃前 {warmup_secs}s)"
    );
    let _ = writeln!(
        out,
        "- 时钟: CLOCK_REALTIME; 单链路绝对延迟含本机 NTP 偏差, 链路间对比不受影响"
    );
    let _ = writeln!(
        out,
        "- 报告 1 按 (coin, time) 去重取首达; 报告 2 按 (time, bbo 内容) 严格匹配\n"
    );

    for s in stats {
        if let Some(reason) = &s.unavailable {
            let _ = writeln!(out, "> ⚠️ 链路 `{}` 不可用, 已跳过: {}", s.name, reason);
        }
    }

    // Report 1: per-feed latency distribution
    let _ = writeln!(out, "\n## 报告 1: 单链路延迟 (local_time - msg.time, ms)\n");
    let mut latency_rows = Vec::new();
    for s in stats {
        if !s.available() {
            let status = if s.unavailable.is_some() {
                "unavailable"
            } else {
                "no data"
            };
            latency_rows.push(vec![
                s.name.to_string(),
                status.to_string(),
                s.raw_msgs.to_string(),
                s.dup_msgs.to_string(),
                s.disconnects.to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
            ]);
            continue;
        }
        let mut sorted = s.latencies_ns.clone();
        sorted.sort_unstable();
        let mut row = vec![
            s.name.to_string(),
            sorted.len().to_string(),
            s.raw_msgs.to_string(),
            s.dup_msgs.to_string(),
            s.disconnects.to_string(),
        ];
        row.extend(dist_cells(&sorted));
        latency_rows.push(row);
    }
    write_table(
        &mut out,
        &[
            "链路",
            "样本(去重)",
            "原始消息",
            "重复",
            "断线",
            "min",
            "p25",
            "p50",
            "p75",
            "p99",
            "max",
        ],
        &latency_rows,
    );

    // Report 2: pairwise first-arrival comparison, strict content matching
    let _ = writeln!(
        out,
        "\n## 报告 2: 跨链路同一更新谁先到 (pairwise, 不受时钟偏差影响)\n"
    );
    let _ = writeln!(
        out,
        "同一更新的判定: `time` 和 bbo 内容 (bid/ask 的 px、sz、n) 完全一致才匹配；\
         自建节点在同一块时间内推送的、其他链路未单独推送的中间状态不参与对比。\n"
    );
    let avail: Vec<&FeedStats> = stats.iter().filter(|s| s.available()).collect();
    if avail.len() < 2 {
        let _ = writeln!(out, "可用链路不足 2 条, 无法做 pairwise 对比。");
        return out;
    }

    let mut pair_rows = Vec::new();
    for i in 0..avail.len() {
        for j in (i + 1)..avail.len() {
            let (a, b) = (avail[i], avail[j]);
            let mut diffs: Vec<i64> = Vec::new(); // b_local - a_local, ns
            let mut a_wins = 0u64;
            let mut b_wins = 0u64;
            let mut ties = 0u64;
            for (key, a_ns) in &a.content_arrival {
                if let Some(b_ns) = b.content_arrival.get(key) {
                    let d = *b_ns as i64 - *a_ns as i64;
                    diffs.push(d);
                    match d.cmp(&0) {
                        std::cmp::Ordering::Greater => a_wins += 1,
                        std::cmp::Ordering::Less => b_wins += 1,
                        std::cmp::Ordering::Equal => ties += 1,
                    }
                }
            }
            if diffs.is_empty() {
                pair_rows.push(vec![
                    format!("{} vs {}", a.name, b.name),
                    "0".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                ]);
                continue;
            }
            diffs.sort_unstable();
            let n = diffs.len() as f64;
            let mut row = vec![
                format!("{} vs {}", a.name, b.name),
                diffs.len().to_string(),
                format!("{} ({:.1}%)", a_wins, a_wins as f64 / n * 100.0),
                format!("{} ({:.1}%)", b_wins, b_wins as f64 / n * 100.0),
                ties.to_string(),
            ];
            row.extend(dist_cells(&diffs));
            pair_rows.push(row);
        }
    }
    let _ = writeln!(
        out,
        "Δ = 右侧链路到达时间 - 左侧链路到达时间 (ms); 正数表示左侧更快。\n"
    );
    write_table(
        &mut out,
        &[
            "对比",
            "匹配样本",
            "左侧先到",
            "右侧先到",
            "同时",
            "Δ min",
            "Δ p25",
            "Δ p50",
            "Δ p75",
            "Δ p99",
            "Δ max",
        ],
        &pair_rows,
    );

    // Overall first-arrival ranking across updates seen (with identical
    // content) by every available feed
    let _ = writeln!(
        out,
        "\n### 全交集首达排名 (仅统计所有可用链路都收到且内容一致的更新)\n"
    );
    let base = avail[0];
    let mut wins = vec![0u64; avail.len()];
    let mut total = 0u64;
    for key in base.content_arrival.keys() {
        let arrivals: Option<Vec<u64>> = avail
            .iter()
            .map(|s| s.content_arrival.get(key).copied())
            .collect();
        let Some(arrivals) = arrivals else { continue };
        total += 1;
        let min = *arrivals.iter().min().unwrap();
        for (k, ns) in arrivals.iter().enumerate() {
            if *ns == min {
                wins[k] += 1;
                break; // equal-ns tie (sub-ns resolution) attributed to first feed in order
            }
        }
    }
    if total == 0 {
        let _ = writeln!(out, "无全交集样本。");
    } else {
        let _ = writeln!(out, "全交集样本: {total}\n");
        let mut ranked: Vec<(usize, u64)> = wins.iter().copied().enumerate().collect();
        ranked.sort_by_key(|(_, w)| std::cmp::Reverse(*w));
        let mut ranking_rows = Vec::new();
        for (k, w) in ranked {
            ranking_rows.push(vec![
                avail[k].name.to_string(),
                w.to_string(),
                format!("{:.1}%", w as f64 / total as f64 * 100.0),
            ]);
        }
        write_table(&mut out, &["链路", "首达次数", "首达率"], &ranking_rows);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{parse_bbo_content, percentile_ns, render_report, FeedStats};

    #[test]
    fn percentile_edges() {
        let sorted: Vec<i64> = (1..=100).collect();
        assert_eq!(percentile_ns(&sorted, 0.0), 1);
        assert_eq!(percentile_ns(&sorted, 50.0), 51); // round(0.5*99)=50 -> value 51
        assert_eq!(percentile_ns(&sorted, 100.0), 100);
        assert_eq!(percentile_ns(&[42], 99.0), 42);
    }

    #[test]
    fn same_content_across_formats_matches() {
        // official array format, sz "70.0"
        let official = r#"{"channel":"bbo","data":{"coin":"ETH","time":1,"bbo":[{"px":"1745.8","sz":"70.0","n":18},{"px":"1745.9","sz":"64.2003","n":24}]}}"#;
        // node object format, numerically identical sz "70"
        let node = r#"{"channel":"bbo","data":{"coin":"ETH","time":1,"bid":{"px":"1745.8","sz":"70","n":18},"ask":{"px":"1745.9","sz":"64.2003","n":24}}}"#;
        assert_eq!(parse_bbo_content(official), parse_bbo_content(node));
        assert!(parse_bbo_content(official).is_some());
    }

    #[test]
    fn different_content_does_not_match() {
        let a = r#"{"channel":"bbo","data":{"coin":"ETH","time":1,"bid":{"px":"1745.8","sz":"70","n":18},"ask":{"px":"1745.9","sz":"64.2003","n":24}}}"#;
        let b = r#"{"channel":"bbo","data":{"coin":"ETH","time":1,"bid":{"px":"1745.8","sz":"71","n":18},"ask":{"px":"1745.9","sz":"64.2003","n":24}}}"#;
        assert_ne!(parse_bbo_content(a), parse_bbo_content(b));
    }

    #[test]
    fn null_side_and_garbage() {
        // one-sided book: official null bid
        let one_sided = r#"{"channel":"bbo","data":{"coin":"X","time":1,"bbo":[null,{"px":"2","sz":"3","n":1}]}}"#;
        let key = parse_bbo_content(one_sided).unwrap();
        assert!(key.0.is_none());
        assert!(key.1.is_some());
        // unparsable content
        assert_eq!(parse_bbo_content("not json"), None);
        assert_eq!(parse_bbo_content(r#"{"channel":"pong"}"#), None);
    }

    #[test]
    fn report_uses_padded_tables_and_pairwise_summary() {
        let stats = vec![
            sample_feed("official", 1_000_200_000),
            sample_feed("quicknode", 1_000_150_000),
        ];
        let report = render_report(&stats, "ETH", 17.0, 5);

        assert!(report.contains("- 有效测量时长: 17s (预热丢弃前 5s)"));
        assert!(report.contains("## 报告 1: 单链路延迟"));
        assert!(report.contains("| 链路      | 样本(去重) | 原始消息 |"));
        assert!(report.contains("| quicknode | 1          | 1        |"));
        assert!(report.contains("## 报告 2: 跨链路同一更新谁先到"));
        assert!(
            report.contains("| 对比                  | 匹配样本 | 左侧先到 | 右侧先到   | 同时 |")
        );
        assert!(
            report.contains("| official vs quicknode | 1        | 0 (0.0%) | 1 (100.0%) | 0    |")
        );
        assert!(!report.contains("### official vs quicknode\n\n- 匹配样本"));
    }

    fn sample_feed(name: &'static str, local_ns: u64) -> FeedStats {
        let mut stats = FeedStats::new(name);
        let time_ms = 1_000;
        let msg = r#"{"channel":"bbo","data":{"coin":"ETH","time":1000,"bbo":[{"px":"1","sz":"2","n":3},{"px":"4","sz":"5","n":6}]}}"#;
        stats.raw_msgs = 1;
        stats
            .latencies_ns
            .push(local_ns as i64 - time_ms * 1_000_000);
        stats.first_arrival.insert(time_ms as u64, local_ns);
        stats
            .content_arrival
            .insert((time_ms as u64, parse_bbo_content(msg).unwrap()), local_ns);
        stats
    }
}
