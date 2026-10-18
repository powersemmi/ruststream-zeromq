---
title: 测试 ZeroMQ 服务
sidebar_position: 7
description: "用 TestApp 测试 ZeroMQ 服务：在进程内运行生产环境的应用，在暂停的时钟上处理延迟，并在回环套接字上运行同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 ZeroMQ 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，可以在进程内，也可以在回环接口的真实套接字上。

被测服务是一个工作者。
它在自己绑定的[队列](../components/broker/zmq-queue.md)上接收任务，并在自己绑定的[广播](../components/broker/zmq-fanout.md)上把每个完成的任务通告为 `jobs.done`。
输入尚未上传的任务会被暂缓：处理器用 [`retry_after`](../components/settlement/retries.md) 要求一分钟后再投递它。
经过三次投递后，被暂缓的任务被丢弃。
应用由 `service(jobs, events)` 构建，因此测试可以选择地址，而 `main` 使用生产环境的地址。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-zeromq` 的 `testing` feature。

## 一个任务及其通告 {/* #a-job-and-its-announcement */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](../components/testing/index.md) 在进程内连接应用的每个 Broker。
测试不打开套接字，也从不绑定这些地址。
测试按生产类型访问 Broker：`tb.broker::<ZmqQueue>()` 是本服务绑定的队列，`tb.broker::<ZmqFanout>()` 是广播。
本服务拨号连接的 Broker 是 `tb.broker::<ZmqQueue<Connect>>()`。
同一类型的两个 Broker 依据 `with_broker_labeled` 的标签区分，通过 `tb.broker_named(..)` 访问。

测试的任务到达的方式，与另一个进程中的对端推送它时一样。
`publish` 在处理器及其通告都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么发布到了 `jobs.done`。
测试主体把已启动的应用作为参数接收，所以下面基于套接字的测试也运行它。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试从暂停的时钟开始，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟向前走。
一分钟的 `HOLD` 不耗费任何时间。
ZeroMQ 不存储任何东西，因此延迟结束时，`retry_after` 把任务的副本发布到它的队列。
副本在消息头中带着重试次数，`published` 把它列在 `jobs` 上，紧挨着测试自己的任务。
第三次投递是挂载点允许的最后一次，因此之后不再发布任何东西。
每次被暂缓的投递都记录为 `Outcome::Nack`。

## 进程内模式模拟了什么 {/* #what-the-in-process-mode-models */}

进程内模式承载套接字所承载的帧，并按模式的规则路由它们：

- 绑定的队列接收对端推送给它的任何消息；拨号连接的订阅轮流接收对端的消息。
- 广播投递给每个名称是消息名称前缀的订阅。
- 请求到达应答方，答案到达发出请求的对端。
- 发往本服务拨号连接的端点，或发往已绑定却没有订阅的端点的发布，会离开本进程去往另一个进程：测试用 `published` 读取它。

套接字的拒绝在进程内同样成立，措辞也与套接字相同。
如果一次发布会被已绑定的队列交回给它自己的订阅，这次发布就被拒绝；已绑定端点上的第二个订阅也被拒绝。
`ack` 和 `nack` 报告 `AckError::Unsupported`，与套接字上相同，因此靠普通 retry 结算的处理器会让它的测试失败。

进程内的投递通道没有上限。
背压、[`read_ahead`](../components/subscriptions/read-ahead.md) 上限、广播的迟到订阅者、等待对端的五秒钟以及重新连接，只存在于真实套接字上。

## 基于套接字的同一个测试 {/* #the-same-test-over-sockets */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 在真实的 ZMTP 套接字上启动同一个应用。
ZeroMQ 不需要服务器，因此测试绑定回环端口，在每次 `cargo test` 中都会运行。
端口零让系统挑选一个空闲端口，因此并行运行的测试永远不会冲突。
测试的任务通过 Broker 自己的发布者发出，穿过一个真实的套接字。
断言与进程内的完全相同，没有改动。

```bash
cargo test
```
