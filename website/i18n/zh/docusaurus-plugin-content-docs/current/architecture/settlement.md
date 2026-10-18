---
title: 结算
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在不结算任何东西的 ZeroMQ 上变成什么：ack 回答 Unsupported，重试是服务自己发布的副本。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "处理器在 ZeroMQ 上的每种结果：投递如何回答，消息变成什么",
  "text": "以文字阅读本图",
  "handlerLane": "处理器",
  "wireLane": "投递",
  "deliveryLane": "消息",
  "ack": ["ack()"],
  "ackWire": ["AckError::", "Unsupported"],
  "ackEffect": ["完成：", "没有要结算", "的东西"],
  "retry": ["retry()"],
  "retryWire": ["AckError::", "Unsupported"],
  "retryEffect": ["消失，除非", "声明了上限：", "那时发副本"],
  "retryAfter": ["retry_after(d)"],
  "retryAfterWire": ["d 之后", "发一个副本"],
  "retryAfterEffect": ["副本是一条", "新消息，", "计入次数"],
  "drop": ["drop()"],
  "dropWire": ["AckError::", "Unsupported"],
  "dropEffect": ["消失"],
  "atMostOnce": ["投递至多一次：处理器没有处理完的消息", "不会再次投递"]
};

export const retries = {
  "title": "ZeroMQ 上的重试：尝试次数上限决定发副本、发往死信目的地还是丢弃",
  "text": "以文字阅读本图",
  "delivery": ["一次投递", "x-ruststream-retry-count 表示这是第几次尝试"],
  "outcome": ["处理器返回", "retry_after(delay)，或在上限下返回 retry()"],
  "cap": ["max_attempts：这次尝试是否低于上限？"],
  "own": ["已绑定：", "副本发往", "自己的", "监听器"],
  "named": ["拨号连接或", "应答方：", "副本发往", "指定的地方"],
  "deadLetter": ["达到上限：", "副本发往", "死信", "名称"],
  "dropped": ["达到上限，", "没有死信", "目的地：", "丢弃"],
  "next": ["副本是一条新", "消息，多一次尝试"],
  "crash": ["副本在进程内等待它的延迟：", "定时器触发前崩溃，副本就会丢失"],
  "handled": "已处理",
  "retries": "重试",
  "below": "低于上限",
  "atCap": "达到上限"
};

# 结算

`ack`、`drop`、`retry` 和 [`retry_after`](../components/settlement/retries.md) 在不结算任何东西的 ZeroMQ 上变成什么：ack 回答 `Unsupported`，重试是服务自己发布的副本。

<Diagram name="settlement" words={table} />

ZeroMQ 套接字交出一条消息后就忘了它。协议中没有确认，因此 `ZmqMessage` 上的 [`ack` 和 `nack`](../components/settlement/index.md) 立即回答 `AckError::Unsupported`。运行时把这个回答记录为警告，无论如何消息都已完成。

普通的 `retry()` 不会再次投递任何东西，处理器也不会被第二次调用。声明了 [`max_attempts`](../components/settlement/retries.md) 或[死信目的地](../components/settlement/dead-letters.md)的注册会把 `retry()` 变成一个副本，和 `retry_after` 一样。

ZeroMQ 不统计投递次数，因此 `redelivery_count` 为空，由运行时自己统计尝试次数。次数随副本的 `x-ruststream-retry-count` 消息头传递，第一次投递算作一次。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

`retry_after` 结果在进程内等待延迟，然后通过注册的[重试发布者](../components/settlement/out-retry.md)发布一个副本。副本发往何处取决于模式和端点所在的一端，由类型来声明。

绑定了端点的 [`ZmqQueue`](../components/broker/zmq-queue.md) 或 [`ZmqFanout`](../components/broker/zmq-fanout.md) 订阅，把自己的名称报告为地址。重试发布者拨号连接这个订阅自己的监听器，副本作为下一次尝试到达那里。挂载点无需指定任何东西。

拨号连接的订阅从一个只发送的对端读取。[`ZmqRpc`](../components/broker/zmq-rpc.md) 应答方的名称不是目的地：它的发布者只发往请求所携带的身份。这两者都没有可供副本使用的地址，因此由挂载点指定副本发往何处，通常是服务为自己绑定的一个队列。没有指定目的地的路由器链无法编译，没有指定目的地的注册会在启动时被拒绝。

达到上限时，投递通过同一个发布者发往死信目的地，因此这个目的地属于另一个端点上的发布者。在队列订阅绑定的端点上，死信会回到这个订阅本身：发布者拒绝它，投递被丢弃。在已绑定的广播上，位于订阅前缀之下的死信名称会回到这个订阅。没有死信目的地时，投递被丢弃，并记录一条警告。
