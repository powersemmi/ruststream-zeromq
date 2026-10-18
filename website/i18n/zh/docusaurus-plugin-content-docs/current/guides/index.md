---
slug: /
sidebar_position: 0
description: "使用 RustStream 的 ZeroMQ Broker ruststream-zeromq 构建 ZeroMQ 服务的指南。"
---

# 指南

`ruststream-zeromq` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的 ZeroMQ Broker，基于纯 Rust 实现的 [zeromq](https://docs.rs/zeromq) 客户端构建。ZeroMQ 中间没有服务器：服务之间通过 `tcp://` 和 `ipc://` 套接字直接通信。

这个 crate 为每种套接字模式提供一个 Broker。`ZmqQueue` 使用 PUSH/PULL 模式，相互竞争的消费者分担工作。`ZmqFanout` 使用 PUB/SUB 模式，每个订阅者都收到一份副本。`ZmqRpc` 使用 DEALER/ROUTER 模式，传递请求及其应答。
