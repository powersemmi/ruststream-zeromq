---
title: 设计
sidebar_position: 2
description: "ruststream-zeromq 的位置：在 RustStream 运行时与 zeromq 客户端之间，每个订阅和每个发布者各有一个套接字，没有服务器。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、zeromq 客户端和对端",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": ["运行时：分发、编解码器、重试，", "在客户端组装的批"],
  "crate": "ruststream-zeromq",
  "subscriptions": ["订阅：一个任务", "把套接字读进", "有界通道"],
  "publishers": ["发布者：一个套接字，", "在第一次发布时", "接入"],
  "brokers": ["ZmqQueue、ZmqFanout、", "ZmqRpc"],
  "lifecycle": ["共享状态：", "端点、已绑定的", "监听器、关闭标记"],
  "wire": ["帧布局", "名称、消息头、载荷"],
  "client": "zeromq 客户端",
  "receiving": ["PULL、SUB、ROUTER"],
  "sending": ["PUSH、PUB、XPUB、", "ROUTER、DEALER"],
  "peers": ["对端：其他进程，任何语言，", "它们之间没有服务器"],
  "testing": ["在 testing 特性下，进程内传输", "可以代替套接字"],
  "mounted": "挂载到",
  "polls": "轮询"
};

# 设计

`ruststream-zeromq` 所处的位置：在 RustStream 运行时与 `zeromq` 客户端之间，每个订阅和每个发布者各有一个套接字，没有服务器。

<Diagram name="design" words={words} />

本 crate 在 [`zeromq`](https://docs.rs/zeromq/0.6.0/zeromq/) 之上实现核心的 Broker trait，`zeromq` 是用 Rust 编写的 ZeroMQ 客户端。它上面的运行时对每个 Broker 都相同。ZeroMQ 特有的一切都在本 crate 里：[三个 Broker](../components/broker/index.md)（每种套接字模式一个）、[帧布局](zeromq/frames.md)、[ack 的含义](../components/settlement/index.md)，以及[重试副本发往何处](../components/settlement/out-retry.md)。

每个 Broker 以类型携带其端点所在的一端：`Bind` 或 `Connect`。这一端决定订阅能否为自己的重试副本寻址，而类型让这成为编译期的事实。[`ZmqRpc`](../components/broker/zmq-rpc.md) 没有这个类型参数：它的副本总是发往挂载点指定的地方。

[订阅](../components/subscriptions/index.md)就是一个名称，Broker 携带 `read_ahead`，其余配置就是端点。[发布](../components/publishing/index.md)只携带自己的帧，因为 ZMTP 发送只接收帧。

每个订阅打开自己的套接字，并由一个任务拥有它。该任务一次读一条消息，放进处理器从中取消息的通道，见[背压](backpressure.md)一页的图。每个发布者在第一次发布时接入自己的套接字。如果本服务为某个订阅绑定了一个端点，那么该端点上的发布者会拨号连接这个订阅的监听器。在广播上，它是一个 XPUB，在接入时等待一次订阅的过滤器，因此它的第一条消息不会被丢弃。

每个套接字都在 Broker 建立连接时所在的运行时上接入，每个任务也在那里运行，无论是哪个运行时发出的请求。运行在独立线程上的处理器可以发布消息，它接入的套接字比该线程的运行时活得更久。

队列发送在发布被丢弃后仍然有效。当对某个对端的发送正在等待时，客户端会把这个对端移出轮转，因此在这一刻被丢弃的发送会永久失去这个对端。发布者把进行中的发送保存在自己的套接字旁边，下一次发布先完成它，再进行自己的发送。本 crate 禁止 `unsafe` 代码，因此由 `selfie` 和 `aliasable` 这两个 crate 持有借用套接字的发送。每个发布者的套接字只装箱一次，一次发布在客户端的开销之外不增加内存分配。

## 进程内 {/* #in-process */}

`testing` 特性增加第二种传输，[`TestApp`](../components/testing/index.md) 连接它来代替套接字。每个 Broker 保留自己的已连接形态、簿记和发布策略，只有底层的套接字换掉了。发布按套接字发布者的方式组帧，订阅按其套接字任务的方式读取帧。

谁收到一条消息，由模式的规则决定。绑定的队列订阅接收推送给它的一切，拨号连接的多个队列订阅轮流接收。广播消息到达名称是其前缀的每个订阅。请求通过对端身份到达应答方，回答返回给提出它的请求。

套接字做出的拒绝在进程内同样成立，措辞也相同。结算在那里同样回答 `Unsupported`。

进程内传输测试的是服务的逻辑，不是传输。订阅预读多远、背压和真实的握手，在同一个测试的[真实连接模式](../components/testing/live.md)下通过环回套接字运行。生产构建不携带其中任何部分：本 crate 在编译期检查投递以及每个句柄的大小。
