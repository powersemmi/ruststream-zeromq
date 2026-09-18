# ruststream-zeromq

**`ruststream-zeromq`** 把 [RustStream](https://powersemmi.github.io/ruststream/) 服务接进一套
ZeroMQ 拓扑，底层是纯 Rust 的 [`zeromq`](https://docs.rs/zeromq) 实现，走 TCP 和 IPC 传输。

中间没有服务器：两个进程直接对话。帧布局写在文档里，因此另一端的对端可以手工拼出消息。一个 Rust
服务就这样加入已有的拓扑，那里可能已经跑着 Python 工作进程或者 C++ 守护进程。

三种套接字模式覆盖三种消息形态：`ZmqQueue`（PUSH/PULL，竞争消费者）、`ZmqFanout`（PUB/SUB，按前缀
过滤的广播）和 `ZmqRpc`（DEALER/ROUTER，请求与响应）。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:app"
```

这个 crate 的参考手册和指南现在是同一页：[docs.rs 上的 `ruststream-zeromq` 总览](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html)
讲了[三种模式](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-three-patterns)、[端点](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#endpoints)、[订阅](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#subscribing)
及其批量与重试、[发布](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#publishing)、非 Rust 对端据以拼装消息的
[传输契约](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-wire-contract)、[生成的文档](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-generated-document)、
[测试](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#testing) 和[运维](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#operations)。

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-transit-connection-horizontal: **[ZeroMQ 传输](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html)** - 三种模式、端点、传输契约、请求与响应，以及测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-zeromq)** - docs.rs 上这个 crate 的 rustdoc。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站点只介绍 ZeroMQ 传输。对每个 Broker 都成立的框架概念（编写订阅者、发布、路由、编解码器、中间件、
可观测性和 CLI）写在 [RustStream 文档](https://powersemmi.github.io/ruststream/)里。
