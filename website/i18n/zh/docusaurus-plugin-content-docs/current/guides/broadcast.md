---
title: 基于 PUB/SUB 的广播
sidebar_position: 3
description: "在 PUB/SUB 上通告事件：在自己绑定的广播上发布的工作者、按前缀订阅的观察者、迟到的观察者和慢观察者。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 基于 PUB/SUB 的广播

本指南在 PUB/SUB 上通告事件：一个服务发布，每个已订阅的观察者都收到一份副本。

示例包含两个服务。
工作者接收任务，并在广播上通告每个完成的任务。
观察者订阅所有任务通告。

## 通告方 {/* #the-announcer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/broadcast.rs#announcer"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/broadcast.rs#announcer"
```

</TabItem>
</Tabs>

工作者在 [`ZmqQueue`](../components/broker/zmq-queue.md) 上读取任务，在 [`ZmqFanout`](../components/broker/zmq-fanout.md) 上发布通告，后者是 PUB/SUB 模式。
挂载两种模式的文件导入本 crate 的 [prelude](../components/broker/prelude.md)，其中每个策略都以它的模式命名：这里是 `ZmqFanoutPublish`。
通告方是稳定的一端，所以它绑定，观察者拨号连接它。
`.out_reply(announce)` 以 `JobDone` 的名称 `jobs.done` 发布每一个 `JobDone`。

## 观察者 {/* #the-watcher */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/broadcast_watcher.rs#watcher"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/broadcast_watcher.rs#watcher"
```

</TabItem>
</Tabs>

在广播上，订阅名称是消息名称的[前缀](../components/subscriptions/prefixes.md)，按字节逐一比较。
`jobs.` 接收 `jobs.done`、`jobs.failed` 和其他任何以 `jobs.` 开头的名称，此外什么也不接收。
`ctx.name()` 是消息到达时所用的名称。
每个拨号连接通告方的观察者，都会收到每条通告的一份副本。

观察者拨号连接，所以它的挂载要指定重试副本发往何处，尽管 `watch` 从不要求重试。
副本发往观察者为自己绑定的一个队列，由同一个处理器读取。
[重试与死信](retries.md)讲解了这条规则。

## 迟到的观察者 {/* #late-watchers */}

广播不存储任何东西。
观察者只收到它的订阅到达发布者之后发布的消息，之前的一概收不到。
启动晚了或重新连接的观察者，会错过中间的通告。
不匹配任何观察者的通告会被丢弃，不报告错误。

向自己的订阅通告的服务不会因此丢失任何东西：它的发布者在第一次发送前就读取了那个订阅的过滤器。

## 慢观察者 {/* #slow-watchers */}

观察者最多领先处理器 [`read_ahead`](../components/subscriptions/read-ahead.md) 次投递读取，默认 1000。
超出后它停止读取，套接字缓冲区随之填满。
这时发布者等待前缀匹配的观察者中最慢的那一个。
因此慢观察者会让通告方放慢，而不会丢失通告。

## 广播上的重试 {/* #retries-on-a-fan-out */}

广播上的重试副本和其他发布一样：它到达每个前缀匹配的订阅。
在服务绑定的广播上，副本会回到原消息的全部受众。
同一端点上的[死信](../components/settlement/dead-letters.md)名称同样匹配前缀：`jobs.dead` 到达 `jobs.`，并被再次处理。
广播的死信发往单独的端点。

## 测试 {/* #testing */}

通告方的测试推送一个任务，读取发布在 `jobs.done` 上的通告。
观察者的测试像通告方那样发布 `jobs.done`、`jobs.failed` 和 `metrics.cpu`，检查 `watch` 收到了前两条。
进程内模式应用前缀规则；迟到的观察者和背压只存在于真实套接字上。
[测试 ZeroMQ 服务](testing.md)讲解了这两种模式。
