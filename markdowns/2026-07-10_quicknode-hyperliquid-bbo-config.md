# QuickNode HyperLiquid BBO 配置调研

## 结论摘要

你的当前 `QUICKNODE_WSS_URL` 配置不适合 QuickNode 文档里的 BBO Book。官方 BBO Book 文档明确写了 `StreamBboBook` 是 `OrderBookStreaming` 的 gRPC method，并且 **not available through JSON-RPC or WebSocket**。所以把当前 benchmark 的 QuickNode 链路当作 `wss://.../hypercore/ws` 去连，不能拿到 BBO Book。

`caerus` 里可用的 QuickNode key 走的是 gRPC L4 order book，不是 WSS。它读取 `QUICKNODE_GRPC_ENDPOINT` / `QUICKNODE_GRPC_TOKEN` 或 connector config 里的 `grpc_endpoint` / `grpc_token`，连 `*.hype-mainnet.quiknode.pro:10000`，然后在 gRPC request metadata 里放 `x-token`。这能证明 key 可用于 QuickNode gRPC，不证明它可用于 QuickNode HyperCore WSS，更不证明 WSS 支持 BBO。

## 调研范围

- QuickNode 官方文档:
  - BBO Book Dataset: https://www.quicknode.com/docs/hyperliquid/datasets/bbo-book
  - Hyperliquid API Endpoints: https://www.quicknode.com/docs/hyperliquid/endpoints
  - Hyperliquid Data Streams: https://www.quicknode.com/docs/hyperliquid/datasets
  - Hyperliquid gRPC Streaming API: https://www.quicknode.com/docs/hyperliquid/grpc-api
- 本地代码:
  - `caerus/framework/src/connectors/interface.rs`
  - `caerus/framework/src/connectors/hyperliquid/grpc/quicknode_l4.rs`
  - `caerus/framework/src/builders/grpc_stream_builder.rs`
  - `caerus/framework/config/connectors.yaml`
  - `hl-md-latency-bench/src/main.rs`

## 关键发现

### 0. 复核官方 Rust SDK: 有 WebSocket,但要分清是哪一种

复核 `/Users/li/RustroverProjects/hyperliquid-sdk/rust` 后,需要修正一个容易误解的说法:不是"Hyperliquid 没有 WebSocket BBO"。官方 Rust SDK 确实有 `Stream::bbo(coin, callback)`,它在 `src/stream.rs:696` 生成:

```rust
let params = json!({"type": "bbo", "coin": coin});
channel: "bbo".to_string()
```

这个走的是 Hyperliquid 官方 public WebSocket 订阅形态:

```json
{"method":"subscribe","subscription":{"type":"bbo","coin":"ETH"}}
```

SDK 的 `src/stream.rs:142` 另有 QuickNode WSS 分支,但只把下面这些 channel 识别为 QuickNode stream:

```rust
"trades" | "orders" | "book_updates" | "twap" | "events" | "writer_actions"
```

`bbo` 不在这个列表里。因此即使 `Stream::new(Some(endpoint))` 传入 QuickNode endpoint,`bbo()` 也不会生成 QuickNode 文档里的 `hl_subscribe` 形态,而是继续生成官方 public WebSocket 的 `subscribe` 形态。SDK 这点和 QuickNode 文档是一致的:QuickNode HyperCore WSS 支持部分 data streams,但 QuickNode BBO Book (`StreamBboBook`) 是 gRPC-only。

### 1. BBO Book 只能走 gRPC

官方 BBO Book 页面写明:

- `StreamBboBook` delivers best bid / best ask。
- gRPC Service 是 `OrderBookStreaming`。
- gRPC Method 是 `StreamBboBook`。
- API Usage 标为 `gRPC Streaming`。
- 文档明确说明: `StreamBboBook` only available via gRPC Streaming API, not JSON-RPC or WebSocket。

QuickNode Data Streams 总览也把 `StreamBboBook` 标为 `gRPC only`。因此当前 benchmark 用 WebSocket 订阅 QuickNode BBO，本身就不匹配这个官方 API。

### 2. QuickNode HyperCore WSS endpoint 不是当前这个 host

官方 Endpoints 文档里 HyperCore WebSocket endpoint 形态是:

```text
wss://your-endpoint.hype-mainnet.quiknode.pro/your-token/hypercore/ws
```

而当前配置形态是:

```text
wss://sleek-light-star.quiknode.pro/<endpoint-token>/
```

这少了 HyperLiquid/HyperCore 专用域名段 `hype-mainnet`，也不是文档里的 `/hypercore/ws` 完整形态。代码会自动补 `/hypercore/ws`，但 host 仍是 `sleek-light-star.quiknode.pro`，不是 `*.hype-mainnet.quiknode.pro`。

实测当前 HTTP RPC URL 对 `eth_chainId` 返回:

```text
401 {"error":"Network mismatch. Consider adding the ChainPrism add-on to your endpoint.", ...}
```

这说明该 endpoint 与请求网络/产品不匹配。WSS 实测 `/hypercore/ws`，无论不加 header、`x-token=QN...`，还是 `x-token=<URL path token>`，都返回 401。

### 3. caerus 使用的是 gRPC L4，不是 WSS

`caerus/framework/src/connectors/interface.rs:23` 定义了:

```rust
pub const QUICKNODE_GRPC_ENDPOINT_ENV: &str = "QUICKNODE_GRPC_ENDPOINT";
pub const QUICKNODE_GRPC_TOKEN_ENV: &str = "QUICKNODE_GRPC_TOKEN";
```

同文件 `ConnectorConfig` 里有:

```rust
grpc_endpoint: Option<String>, // for Quicknode HyperLiquid gRPC, e.g. endpoint.hype-mainnet.quiknode.pro:10000
grpc_token: Option<String>,    // for Quicknode HyperLiquid gRPC x-token auth
```

`caerus/framework/src/connectors/hyperliquid/grpc/quicknode_l4.rs:107` 把 endpoint 归一化为 `https://...` 后创建 tonic gRPC channel；`quicknode_l4.rs:208` 创建 `L4BookRequest`；`quicknode_l4.rs:209` 把 token 插入 metadata:

```rust
request.metadata_mut().insert("x-token", token);
```

`caerus/framework/config/connectors.yaml:71` 也注释说明这是 QuickNode HyperCore gRPC L4 order book endpoint，示例 endpoint 是:

```yaml
grpc_endpoint: quicknode-api-name.hype-mainnet.quiknode.pro:10000
grpc_token: grpc_token
```

所以 `caerus` 的可用性只覆盖 gRPC L4。它没有证明同一个 key 能拿 QuickNode WSS，更没有证明能用 WSS 拿 BBO Book。

### 4. 当前 benchmark 的 QuickNode 设计与 BBO Book 文档不一致

`hl-md-latency-bench/src/main.rs:160` 目前把 `QUICKNODE_WSS_URL` 归一化为:

```text
wss://host/token/hypercore/ws
```

然后用 WebSocket 发送 Hyperliquid 官方格式:

```json
{"method":"subscribe","subscription":{"type":"bbo","coin":"ETH"}}
```

这跟 QuickNode HyperCore WSS 文档里的订阅格式也不同。QuickNode HyperCore WSS 示例使用:

```json
{
  "method": "hl_subscribe",
  "params": {
    "streamType": "trades",
    "filters": {"coin": ["BTC", "ETH"]}
  }
}
```

即使修正为 QuickNode WSS 订阅格式，BBO Book 仍不可用，因为 BBO Book 是 gRPC only。

## 对当前配置的判断

### QUICKNODE_API_KEY

`QUICKNODE_API_KEY=QN_...` 这个值不应该被理解为当前 WSS URL path token。官方 gRPC 文档的例子把 HTTP Provider URL 里的 path token 拆出来作为 token；文档也说 token 可以来自 Endpoint Security tab。

如果你说该 key 在 `caerus` 可用，那么它很可能是作为 `QUICKNODE_GRPC_TOKEN` 被发送到 gRPC metadata 的 `x-token`，用于 `*.hype-mainnet.quiknode.pro:10000`。这不等于它可用于 `wss://sleek-light-star.quiknode.pro/.../hypercore/ws`。

### QUICKNODE_WSS_URL

当前 `QUICKNODE_WSS_URL` 基本可以判定有问题:

```text
wss://sleek-light-star.quiknode.pro/<endpoint-token>/
```

如果要测试 QuickNode HyperCore WSS，文档形态应是:

```text
wss://<endpoint>.hype-mainnet.quiknode.pro/<token>/hypercore/ws
```

但这条也不能用于 BBO Book，因为 BBO Book 不支持 WSS。

### QUICKNODE_RPC_URL

当前 `QUICKNODE_RPC_URL` 对 HyperLiquid 请求返回 `Network mismatch`，说明 endpoint 产品/网络不匹配。HyperCore JSON-RPC 文档形态应包含 `/hypercore`，Info API 应包含 `/info`，gRPC streaming 则走 `:10000` 端口。

## 建议

1. 如果目标是对比 QuickNode BBO 与 official/ob 的 BBO 延迟，应该把 benchmark 的 QuickNode 链路改成 gRPC `StreamBboBook`，不要继续走 WSS。

2. 新增或改用下面这类配置:

```bash
export QUICKNODE_GRPC_ENDPOINT='<endpoint>.hype-mainnet.quiknode.pro:10000'
export QUICKNODE_GRPC_TOKEN='<token>'
```

`<token>` 应优先使用你在 `caerus` 已验证可用的 `QUICKNODE_GRPC_TOKEN`。如果你只有 Provider URL，则按官方示例从:

```text
https://<endpoint>.hype-mainnet.quiknode.pro/<token>/
```

拆成:

```text
QUICKNODE_GRPC_ENDPOINT='<endpoint>.hype-mainnet.quiknode.pro:10000'
QUICKNODE_GRPC_TOKEN='<token>'
```

3. 当前 `QUICKNODE_WSS_URL` 可以从 BBO benchmark 中移除，或保留为非 BBO stream 的可选实验项。但不要用它承载 `StreamBboBook`。

4. `caerus` 当前 proto 只有 `StreamL2Book` / `StreamL4Book`，没有 `StreamBboBook`。如果复用 `caerus` 方式，需要更新 `orderbook.proto` 并新增 BBO parser，或在本 benchmark 内单独生成最小 gRPC client。

## 最小后续实现方向

最小可行改法不是继续修 WebSocket header，而是:

- 在 `Cargo.toml` 加 `tonic` / `prost` / `tonic-build` 等 gRPC 依赖。
- 增加 QuickNode gRPC feed，读取 `QUICKNODE_GRPC_ENDPOINT` 和 `QUICKNODE_GRPC_TOKEN`。
- 调用 `OrderBookStreaming.StreamBboBook({ coins: [coin] })`。
- 把 gRPC `BboBookUpdate` 转成当前 aggregator 可比较的 bbo JSON 或直接扩展 `Event::Sample` 结构。

跳过: 继续适配 QuickNode WSS 的 BBO 订阅。官方文档已经说明 BBO Book 不走 WSS。
