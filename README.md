# hl-md-latency-bench

对比 4 条 HyperLiquid bbo 行情链路的延迟:

| 链路 | 来源 |
|---|---|
| official | `wss://api.hyperliquid.xyz/ws` |
| quicknode | `$QUICKNODE_WSS_URL` → `…/hypercore/ws`(官方订阅格式透传) |
| ob | `wss://<redacted-ob-host>/ws?token=…` |
| obaws | `wss://<redacted-obaws-host>/ws?token=…` |

连不上的链路(重试 3 次后)自动标记 unavailable 并跳过,其余链路照常对比。

## 运行

```bash
export QUICKNODE_WSS_URL='wss://<endpoint>.quiknode.pro/<token>/'   # 缺失则跳过 quicknode
export HL_NODE_TOKEN='<自建节点 token>'                              # 可选,有默认值

cargo run --release                          # 默认 ETH,10 分钟
cargo run --release -- --coin ETH --duration-secs 60   # 短测
```

Ctrl-C 可提前结束并输出报告。报告打印到 stdout 并写入 `report_<时间戳>.md`;
逐条消息的原始记录(feed / time_ms / local_ns / latency_ns)经 tracing-appender
异步写入 `logs/bench.log`,可供离线复查。

## 公平性设计

- 四条链路使用完全相同的客户端代码:同步 tungstenite + 每链路一条专用 OS 线程;
- `read()` 返回后立即用 `CLOCK_REALTIME` 打戳,之后才做解析;
- 热路径只做字节扫描提取 `time` 字段,无 serde、无分配;
- 日志走非阻塞 appender,不阻塞收包线程;
- 测量开始前丢弃预热期(默认 5s)样本;
- 样本按 time 去重(取首达);断线自动重连,断线次数在报告中标注。

报告 1 是各链路 `local_time − msg.time` 的 min/p25/p50/p75/p99/max(绝对值含本机
NTP 偏差,链路间对比不受影响);报告 2 按 `time` 匹配同一更新,统计两两先到率与
到达时差分布,完全不受时钟偏差影响。

## 部署到 Linux 服务器

TLS 用 rustls,无系统依赖,rsync 源码后直接构建:

```bash
rsync -a --exclude target ./ <server>:~/hl-md-latency-bench/
ssh <server> 'cd ~/hl-md-latency-bench && cargo build --release'
```
