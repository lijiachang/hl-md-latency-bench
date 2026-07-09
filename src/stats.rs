use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::feed::Event;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

pub struct FeedStats {
    pub name: &'static str,
    pub subscribed: bool,
    pub unavailable: Option<String>,
    pub raw_msgs: u64,
    pub dup_msgs: u64,
    pub warmup_dropped: u64,
    pub disconnects: u32,
    /// latency (local receive − exchange time), ns, one entry per unique `time_ms`
    pub latencies_ns: Vec<i64>,
    /// time_ms → first-arrival local timestamp (ns)
    pub first_arrival: HashMap<u64, u64>,
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
            disconnects: 0,
            latencies_ns: Vec::new(),
            first_arrival: HashMap::new(),
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
            }) => {
                let s = &mut stats[feed];
                s.raw_msgs += 1;
                if local_ns < measure_start_ns {
                    s.warmup_dropped += 1;
                } else if let std::collections::hash_map::Entry::Vacant(e) =
                    s.first_arrival.entry(time_ms)
                {
                    e.insert(local_ns);
                    s.latencies_ns
                        .push(local_ns as i64 - time_ms as i64 * 1_000_000);
                } else {
                    s.dup_msgs += 1;
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

fn dist_row(sorted: &[i64]) -> String {
    format!(
        "{} | {} | {} | {} | {} | {}",
        ms(sorted[0]),
        ms(percentile_ns(sorted, 25.0)),
        ms(percentile_ns(sorted, 50.0)),
        ms(percentile_ns(sorted, 75.0)),
        ms(percentile_ns(sorted, 99.0)),
        ms(sorted[sorted.len() - 1]),
    )
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
        "- 有效测量时长: {measured_secs:.0}s(预热丢弃前 {warmup_secs}s)"
    );
    let _ = writeln!(
        out,
        "- 时钟: CLOCK_REALTIME;单链路绝对延迟含本机 NTP 偏差,链路间对比不受影响"
    );
    let _ = writeln!(out, "- 样本按 (coin, time) 去重,同一 time 只取首达消息\n");

    for s in stats {
        if let Some(reason) = &s.unavailable {
            let _ = writeln!(out, "> ⚠️ 链路 `{}` 不可用,已跳过: {}", s.name, reason);
        }
    }

    // Report 1: per-feed latency distribution
    let _ = writeln!(
        out,
        "\n## 报告 1:单链路延迟(local_time − msg.time,单位 ms)\n"
    );
    let _ = writeln!(
        out,
        "| 链路 | 样本(去重) | 原始消息 | 重复 | 断线 | min | p25 | p50 | p75 | p99 | max |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|---|---|---|---|");
    for s in stats {
        if !s.available() {
            let status = if s.unavailable.is_some() {
                "unavailable"
            } else {
                "no data"
            };
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} | - | - | - | - | - | - |",
                s.name, status, s.raw_msgs, s.dup_msgs, s.disconnects
            );
            continue;
        }
        let mut sorted = s.latencies_ns.clone();
        sorted.sort_unstable();
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} |",
            s.name,
            sorted.len(),
            s.raw_msgs,
            s.dup_msgs,
            s.disconnects,
            dist_row(&sorted),
        );
    }

    // Report 2: pairwise first-arrival comparison
    let _ = writeln!(
        out,
        "\n## 报告 2:跨链路同一更新谁先到(pairwise,不受时钟偏差影响)\n"
    );
    let avail: Vec<&FeedStats> = stats.iter().filter(|s| s.available()).collect();
    if avail.len() < 2 {
        let _ = writeln!(out, "可用链路不足 2 条,无法做 pairwise 对比。");
        return out;
    }

    for i in 0..avail.len() {
        for j in (i + 1)..avail.len() {
            let (a, b) = (avail[i], avail[j]);
            let mut diffs: Vec<i64> = Vec::new(); // b_local - a_local, ns
            let mut a_wins = 0u64;
            let mut b_wins = 0u64;
            let mut ties = 0u64;
            for (time_ms, a_ns) in &a.first_arrival {
                if let Some(b_ns) = b.first_arrival.get(time_ms) {
                    let d = *b_ns as i64 - *a_ns as i64;
                    diffs.push(d);
                    match d.cmp(&0) {
                        std::cmp::Ordering::Greater => a_wins += 1,
                        std::cmp::Ordering::Less => b_wins += 1,
                        std::cmp::Ordering::Equal => ties += 1,
                    }
                }
            }
            let _ = writeln!(out, "### {} vs {}\n", a.name, b.name);
            if diffs.is_empty() {
                let _ = writeln!(out, "无共同 time 样本。\n");
                continue;
            }
            diffs.sort_unstable();
            let n = diffs.len() as f64;
            let _ = writeln!(out, "- 匹配样本: {}", diffs.len());
            let _ = writeln!(
                out,
                "- {} 先到: {} ({:.1}%),{} 先到: {} ({:.1}%),同时: {}",
                a.name,
                a_wins,
                a_wins as f64 / n * 100.0,
                b.name,
                b_wins,
                b_wins as f64 / n * 100.0,
                ties
            );
            let _ = writeln!(
                out,
                "- 到达时差 ({} − {},ms,正数 = {} 更快):",
                b.name, a.name, a.name
            );
            let _ = writeln!(out, "\n| min | p25 | p50 | p75 | p99 | max |");
            let _ = writeln!(out, "|---|---|---|---|---|---|");
            let _ = writeln!(out, "| {} |\n", dist_row(&diffs));
        }
    }

    // Overall first-arrival ranking across times seen by every available feed
    let _ = writeln!(out, "### 全交集首达排名(仅统计所有可用链路都收到的 time)\n");
    let base = avail[0];
    let mut wins = vec![0u64; avail.len()];
    let mut total = 0u64;
    for time_ms in base.first_arrival.keys() {
        let arrivals: Option<Vec<u64>> = avail
            .iter()
            .map(|s| s.first_arrival.get(time_ms).copied())
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
        let _ = writeln!(out, "| 链路 | 首达次数 | 首达率 |");
        let _ = writeln!(out, "|---|---|---|");
        let mut ranked: Vec<(usize, u64)> = wins.iter().copied().enumerate().collect();
        ranked.sort_by_key(|(_, w)| std::cmp::Reverse(*w));
        for (k, w) in ranked {
            let _ = writeln!(
                out,
                "| {} | {} | {:.1}% |",
                avail[k].name,
                w,
                w as f64 / total as f64 * 100.0
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::percentile_ns;

    #[test]
    fn percentile_edges() {
        let sorted: Vec<i64> = (1..=100).collect();
        assert_eq!(percentile_ns(&sorted, 0.0), 1);
        assert_eq!(percentile_ns(&sorted, 50.0), 51); // round(0.5*99)=50 -> value 51
        assert_eq!(percentile_ns(&sorted, 100.0), 100);
        assert_eq!(percentile_ns(&[42], 99.0), 42);
    }
}
