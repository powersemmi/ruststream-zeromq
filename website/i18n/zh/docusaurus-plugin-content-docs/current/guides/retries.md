---
title: 重试与死信
sidebar_position: 5
description: "在至多投递一次、不存储任何东西的传输上，稍后重试 ZeroMQ 消息，限定尝试次数，并转存失败的消息。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信

本指南稍后重试一条消息，限定尝试次数，并把失败的消息转存到[死信](../components/settlement/dead-letters.md)队列。

ZeroMQ 对一条消息至多投递一次，并且不存储任何东西。
处理器处理失败的消息就此消失，除非服务再次发布它。
这件事由框架来做：挂载点声明上限和副本的去向，每个副本都在消息头中带着重试次数。

示例是一个计费服务，有两个工作者。
任务工作者把失败的任务重试到自己的队列中。
发票工作者把失败的发票转存到单独的队列，留给人来查看。

## 至多一次 {/* #at-most-once */}

[`ack`](../components/settlement/index.md) 和 `nack` 没有什么可以告诉套接字的：ZeroMQ 没有确认。
它们都报告 `AckError::Unsupported`，运行时记录一条警告。
因此不带上限的普通 `retry` 什么也带不回来，处理器也不会被再次调用。
必须发生的重试要通过副本完成，如下所示。

## 任务工作者 {/* #the-job-worker */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#jobs"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#jobs"
```

</TabItem>
</Tabs>

失败的任务用 [`retry_after`](../components/settlement/retries.md) 要求 30 秒后再试一次。
运行时等待延迟过去，然后发布这次投递的副本，载荷和消息头保持到达时的样子。
副本在服务的内存中等待，因此在延迟结束前停止的进程会丢失它。

## 发票工作者 {/* #the-invoice-worker */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#invoices"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#invoices"
```

</TabItem>
</Tabs>

没有金额的发票无法发出，再发一次也不会改变什么。
`send_invoice` 返回 `retry`，挂载点把它变成死信。
`park` 读取死信，并把每一条记入日志。

## 挂载点 {/* #the-mount-sites */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 统计一个任务的每次投递，第一次也算在内。
ZMTP 没有投递计数器，因此框架通过 `x-ruststream-retry-count` 消息头计数，无论副本经由何处传递，上限都成立。
失败任务的第五次投递不再重试；没有死信目的地时，它被丢弃，并记录一条写明订阅的警告。

[`out_retry`](../components/settlement/out-retry.md) 选定发出副本的发布者。
`jobs` 队列由本服务绑定，因此 `.out_retry(Publish)` 把每个副本送回这个队列，由空闲的任务工作者接收。

发票的挂载声明了 `dead_letter("invoices.dead")`，没有上限，因此每张失败的发票都直接进入那里。
死信是一次普通的发布，而已绑定的队列只接受发往它自己订阅名称的发布。
因此死信通过 `parked` 发出，这是 `dead` Broker 上的一个策略，`park` 在那里读取它们。
同一挂载的重试副本也通过这个发布者发出，因此一个挂载有了死信，就失去了重试。
想要两者兼得的挂载，把重试留在自己的队列上，不配置死信。

## 拨号连接的工作者 {/* #a-worker-that-dials */}

拨号连接分发器的工作者无法把副本送回去：分发器只发送。
它的挂载用 `.out_retry(policy).to("jobs")` 指定副本的去向，即工作者为自己绑定的一个队列，并把同一个处理器挂载到这个队列上。
没有指定目的地的挂载会在启动时被拒绝，没有指定目的地的 [`Router`](api:ruststream::runtime::Router) 链无法编译。
[`out_retry`](../components/settlement/out-retry.md) 一页展示了这种挂载，[基于 PUSH/PULL 的流水线](pipelines.md)用到了它。

## 测试延迟 {/* #testing-the-delays */}

测试在进程内运行应用。
任务测试从暂停的时钟开始，因此 `tb.advance(BACKOFF)` 让 30 秒瞬间过去。
它检查五次投递，以及最后一个带有重试次数 4 的副本。
发票测试检查发票在一次投递之后到达了 `park`。
[测试 ZeroMQ 服务](testing.md)讲解了测试套件。
