---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-zeromq，在 PULL 套接字上运行第一个工作者，从 Python 对端向它发送任务，并在不打开套接字的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-zeromq`，运行第一个工作者，并从另一个进程中的对端向它发送任务。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-zeromq = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-zeromq` 是 ZeroMQ Broker。
它的 `testing` feature 加入测试所用的[进程内模式](../components/testing/index.md)。

这个 Broker crate 有以下 feature：

| Feature | 提供的内容 |
|---|---|
| `asyncapi` | 生成的 AsyncAPI 文档中的 [`x-ruststream-zeromq` 扩展](../components/asyncapi/index.md) |
| `testing` | `TestApp` 背后的进程内模式 |

ZeroMQ 没有服务器，因此不需要安装或启动任何东西。
本 crate 通过 [zeromq](https://docs.rs/zeromq) 使用 ZMTP 协议通信，这是一个用 Rust 编写的客户端。
因此构建既不需要 libzmq，也不需要 C 编译器。
两个进程直接通信，经由 `tcp://`，或者经由 `ipc://`，即同一主机上的 Unix 套接字。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_job` 从 `jobs` 读取 JSON 任务。
它确认带有条目的任务，丢弃没有条目的任务。
[`ZmqQueue`](../components/broker/zmq-queue.md) 是 PUSH/PULL 模式：每个任务只到达一个工作者。
[`ZmqEndpoint::bind`](../components/broker/endpoint/bind.md) 表示本进程在这个地址上监听。
`ZmqQueue::new` 只记录端点；套接字在服务启动时才打开。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

## 对端 {/* #the-peer */}

任何使用 ZMTP 协议的进程都可以向工作者推送任务。
一条消息由三帧组成：名称、消息头和载荷（[线上格式](../components/wire/index.md)）。
下面这个 Python 对端使用 [pyzmq](https://pyzmq.readthedocs.io/)：

```python title="push_job.py"
import json
import zmq

push = zmq.Context().socket(zmq.PUSH)
push.connect("tcp://127.0.0.1:5555")
push.send_multipart([b"jobs", b"", json.dumps({"id": 1, "items": 3}).encode()])
push.close(linger=1000)
```

在另一个终端里运行它：

```bash
python3 push_job.py
```

服务记录一条 `job taken` 日志，其中带有任务的 id 和条目数。
紧接着是一条警告 `ack / nack failed`：这个 Broker 不支持确认。
ZeroMQ 在线路上没有确认，因此工作者读到任务时，任务就算完成（[确认](../components/settlement/index.md)）。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。
对端也可以是一个 RustStream 服务：[基于 PUSH/PULL 的流水线](pipelines.md)构建了一个把结果推送给另一个服务的服务。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，推送一个任务，然后检查 `take_job` 如何结算它。
在测试中，Broker 运行于进程内，因此测试不打开任何套接字：

```bash
cargo test
```

[测试 ZeroMQ 服务](testing.md)讲解测试套件，以及在真实套接字上运行的同一个测试。

## 从 pyzmq 或 libzmq 迁移 {/* #coming-from-pyzmq-or-libzmq */}

基于 ZeroMQ 语言绑定的服务要自己打开套接字，自己编写接收循环。
下表列出每一部分在 RustStream 中的位置：

| pyzmq 或 libzmq | RustStream |
|---|---|
| `ctx.socket(zmq.PULL)` 和 `bind` | [`ZmqQueue::new(ZmqEndpoint::bind(..))`](../components/broker/zmq-queue.md) 加一个 `#[subscriber]` 处理器 |
| `connect` | [`ZmqEndpoint::connect`](../components/broker/endpoint/connect.md)；连接的哪一端是 Broker 类型的一部分 |
| `recv_multipart` 循环 | 处理器，每条消息调用一次 |
| 解码载荷帧 | 处理器的参数类型，由编解码器解码 |
| `send_multipart` | 回复或 `Out` 参数，经由[发布策略](../components/publishing/index.md)发送 |
| `setsockopt(zmq.SUBSCRIBE, prefix)` | [`ZmqFanout`](../components/broker/zmq-fanout.md) 上的订阅名称（[基于 PUB/SUB 的广播](broadcast.md)） |
| REQ 和 REP | 基于 DEALER 和 ROUTER 的 [`ZmqRpc`](../components/broker/zmq-rpc.md)（[请求与回复](request-reply.md)） |
| `tcp://*:5555` | `tcp://0.0.0.0:5555` |
| `inproc://` | 测试用 [`TestApp`](../components/testing/index.md)；同一主机上的进程之间用 `ipc://` |
| `SNDHWM` 和 `RCVHWM` | [`read_ahead`](../components/subscriptions/read-ahead.md)（[在生产环境中运行](production.md)） |
| 监视多个套接字的 `zmq.Poller` | 每个套接字一个订阅，各自运行在自己的任务上 |
