/// Wall-clock timestamp in nanoseconds since the Unix epoch (CLOCK_REALTIME).
///
/// Same clock source as caerus `framework/src/timer.rs::now_realtime`; the exchange
/// `time` field is milliseconds from the same epoch, so latency is
/// `now_realtime_ns() - time_ms * 1_000_000`. Absolute values include local NTP
/// offset; comparisons between feeds sharing this clock are unaffected.
pub fn now_realtime_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts);
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}
