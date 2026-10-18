---
title: 请求与回复
sidebar_position: 4
description: "在 DEALER/ROUTER 上用普通的回复处理器应答请求，并在处理器中向另一个服务发出请求，带超时和稍后重试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 请求与回复

本指南在 DEALER/ROUTER 上应答请求，并在处理器内部向另一个服务发出请求。

示例包含两个服务。
评分服务回答某个客户的订单金额是否获批。
结账服务为它接收的每个订单向评分服务发出询问。

## 应答方 {/* #the-responder */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

[`ZmqRpc`](../components/broker/zmq-rpc.md) 是 DEALER/ROUTER 模式。
应答方是稳定的一端，所以它绑定，每个请求方都拨号连接它。
`score` 是一个普通的回复处理器：它返回答案，或者丢弃没有金额的请求。

ROUTER 套接字知道每个请求来自哪个对端。
它把这个对端以 `reply-to` 消息头的形式标在请求上，答案必须发往那里。
因此 `Score` 不声明自己的目的地，`reply("reply")` 只指定一个后备目的地。
下面的转换为每个请求替换它。

应答方要指定它的[重试副本](../components/settlement/out-retry.md)发往何处：`.out_retry(Publish).to("score.retry")`。
请求的副本没有自己的地址，因此缺少这一步的挂载会在启动时被拒绝。
应答方也不接收批：在它上面调用 `.batch(..)` 无法编译，因为每个答案都发往各自的对端。

## 转换 {/* #the-transform */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#transform"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#transform"
```

</TabItem>
</Tabs>

`ReplyToRequester` 是作用于回复的[发布转换](../components/request-reply/responders.md)。
它读取请求的 `reply-to` 消息头，把答案发往那里。
它还复制请求的 `correlation-id`，请求方依据它匹配答案。
它声明 `Destination = Names`，因为目的地由它选择。
只有在回复类型没有声明目的地的地方，转换才可以这样做。
因此带名称的回复类型和这个转换无法一起编译。

发给已经离开的对端的答案会被拒绝，错误中写明它的地址。
应答方由此得知它的答案无处可达；而在广播上，不匹配的消息会被悄无声息地丢弃。

## 请求方 {/* #the-requester */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply_client.rs#requester"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply_client.rs#requester"
```

</TabItem>
</Tabs>

`check` 接收一个 `Out<impl RequestReply>` 参数，挂载点用 `.out(DefaultSlot, ask)` 把它绑定到一个 `ZmqRpc` 策略。
`scoring` Broker 拨号连接评分服务，没有订阅；它只发出请求。
[`request`](../components/request-reply/index.md) 通过自己的 DEALER 套接字发送请求，并返回带有相同 `correlation-id` 的答案。
`check` 自己编码请求、解码答案，所用的 JSON 与评分服务读写的一致。

## 超时 {/* #timeouts */}

在 `ASK_TIMEOUT` 内没有得到答案的请求以 [`ZmqError::RequestTimeout`](../components/broker/errors.md) 失败。
这时 `check` 返回 `retry_after(ASK_AGAIN)`，订单在十秒后回来。
`max_attempts` 把尝试次数限制为三次。
这就是 [ZeroMQ 指南](https://zguide.zeromq.org/docs/chapter4/#Client-Side-Reliability-Lazy-Pirate-Pattern)中的 Lazy Pirate 模式，由框架负责计数。
超时的请求可能已经到达了应答方，因此请求必须可以安全地发送两次。

`request` 和回复发布都不是取消安全的。
处理器通过请求的超时来放弃，而不是通过 `select!` 分支（[取消](../components/publishing/cancellation.md)）。

## 测试 {/* #testing */}

评分服务的测试像请求方的 DEALER 那样发送请求，并检查 `score` 如何结算它们。
结账服务的测试在进程内运行，没有评分服务，因此它的请求会超时。
测试检查 `check` 要求了重试，并且副本在 `ASK_AGAIN` 之后回来，重试次数为一。
时钟是暂停的，因此超时和延迟瞬间就过去了。
[测试 ZeroMQ 服务](testing.md)讲解了测试套件。
