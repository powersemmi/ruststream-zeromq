---
title: 基于 PUSH/PULL 的流水线
sidebar_position: 2
description: "在 PUSH/PULL 上构建由分发器、工作者和汇集器组成的流水线：谁来绑定、相互竞争的工作者、发往汇集器的结果，以及批。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 基于 PUSH/PULL 的流水线

本指南构建 [ZeroMQ 指南](https://zguide.zeromq.org/docs/chapter1/#Divide-and-Conquer)中的流水线：分发器推送任务，工作者争抢任务，汇集器收集结果。

示例包含两个服务。
工作者从分发器取得任务，并为每个任务向汇集器推送一个结果。
汇集器按批收集结果。
分发器可以是任何推送任务的对端，比如[快速开始](getting-started.md)中的 Python 对端。

## 谁来绑定 {/* #who-binds */}

连接中稳定的一端绑定，来来去去的一端拨号连接。
在流水线中，分发器和汇集器是稳定的，所以它们绑定。
工作者拨号连接这两者，多一个工作者进程就多一次拨号。

[`ZmqEndpoint::bind`](../components/broker/endpoint/bind.md) 和 [`ZmqEndpoint::connect`](../components/broker/endpoint/connect.md) 声明本服务位于哪一端。
这一端是 Broker 类型的一部分：基于 `connect` 构建的 Broker 是 `ZmqQueue<Connect>`。
它决定 Broker 可以发布什么，以及重试副本可以发往何处。
因此错误会在编译期或启动时暴露，而不是在生产环境中。

## 工作者 {/* #the-worker */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/pipelines.rs#work"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/pipelines.rs#work"
```

</TabItem>
</Tabs>

`work` 接收一个任务，返回一个 `Done`。
`Done` 自己指定了目的地 `results`，因此订阅者子句只带一个单独的 `reply`。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/pipelines.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/pipelines.rs#app"
```

</TabItem>
</Tabs>

`ventilator` Broker 拨号连接分发器，它的订阅读取任务。
拨号连接同一个分发器的工作者相互竞争：PUSH 把每个任务轮流交给其中一个工作者。

结果通过第二个 Broker `sink` 发出，它拨号连接汇集器。
`bindable()` 把 Broker 变成可以绑定发布策略的 Broker，`sink.bind(Publish)` 就是一个通过它发布的策略。
`.out_reply(results)` 把每个 `Done` 发往那里。
工作者不能通过 `ventilator` Broker 回复：分发器只发送，因此订阅一旦拨号连接，这个 Broker 就拒绝每一次发布。
[回复](../components/publishing/replies.md)一页讲解了这些拒绝。

拨号连接的订阅还要指定它的[重试副本](../components/settlement/out-retry.md)发往何处，即使它的处理器从不要求重试。
这里副本发往 `retries`，这是工作者为自己绑定的一个队列，由同一个处理器读取。
副本带着名称 `jobs`，也就是把它接回来的那个订阅的名称。
[重试与死信](retries.md)完整讲解了重试。

## 汇集器 {/* #the-sink */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/pipeline_sink.rs#sink"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/pipeline_sink.rs#sink"
```

</TabItem>
</Tabs>

汇集器绑定工作者拨号连接的地址。
已绑定的 PULL 套接字接收任何工作者推送给它的消息，因此一个订阅就能读取所有工作者的结果。
`collect` 接收一个切片，因此它收到的是[批](../components/subscriptions/batches.md)。
一批攒满 32 个结果时结束，或者在第一个结果之后 20 毫秒结束，以先到者为准。
线路上的帧保持不变：批在汇集器一端聚集。

## 每个端点一种工作 {/* #one-kind-of-work-per-endpoint */}

PUSH 和 PULL 不看名称帧。
已绑定的队列把推送给它的每条消息都交给它的订阅，不管名称是什么。
因此第二种工作，比如任务之外的报表，要有自己的端点和自己的 Broker。
在已绑定的端点上挂载第二个订阅会在启动时被拒绝，错误中写明两个订阅。

## 顺序与键 {/* #order-and-keys */}

PUSH 把消息轮流交给已连接的工作者，不读取键。
同一个客户的两个任务可能同时运行在两个工作者上。
必须按顺序看到同一客户任务的处理器，要运行在一个工作者进程中。
在一个进程内，挂载点上的 [`workers`](api:ruststream::runtime::Workers) 让处理器同时处理多次投递。
ZeroMQ 消息不带键，因此用 `by_key` 按键划分的工作分区轮流接收它的投递。

## 测试流水线 {/* #testing-the-pipeline */}

每个示例文件都有一个用 `TestApp::start` 启动其服务的测试。
工作者的测试像分发器那样推送一个任务，然后读取通过 `sink` Broker 发布到 `results` 的结果。
汇集器的测试一次推送三个结果，检查 `collect` 把它们作为一批收到。
[测试 ZeroMQ 服务](testing.md)讲解了测试套件。
