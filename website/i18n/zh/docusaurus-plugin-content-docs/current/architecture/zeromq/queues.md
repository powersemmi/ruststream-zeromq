---
title: 队列
sidebar_position: 2
description: "zeromq 客户端为每个对端排队最多 10 万条消息，消息一进入队列，发送就结束；本 crate 的发布者远在此之前就会等待。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "在两个 zeromq 套接字之间，消息在哪里等待：PUSH/PULL 与 PUB/SUB",
  "text": "以文字阅读本图",
  "pushPull": "PUSH/PULL",
  "pubSub": "PUB/SUB",
  "pushsend": ["PUSH 发送", "选出轮到的", "那个对端"],
  "pushwrite": ["排入该对端的", "队列，由该对端的", "任务写进连接"],
  "pushkernel": ["内核的", "套接字缓冲区，", "两端都有"],
  "pushrecv": ["PULL 接收", "轮流读取各对端", "的连接"],
  "pubsend": ["PUB 发送", "发给过滤器匹配的", "每个订阅者"],
  "pubwrite": ["为每个订阅者排入", "一份副本；队列满时", "丢弃该副本"],
  "pubkernel": ["内核的", "套接字缓冲区，", "两端都有"],
  "pubrecv": ["SUB 接收", "读取过滤器", "放行的消息"],
  "client": ["zeromq 客户端为每个对端排队最多 10 万条消息：", "消息一入队，一次发送就结束"],
  "libzmq": ["libzmq 为每个连接排队 1000 条消息", "（SNDHWM、RCVHWM）：到达高水位时 PUB 丢弃，PUSH 阻塞"],
  "inTurn": "轮流"
};

# 队列

`zeromq` 客户端为每个对端排队最多 10 万条消息，消息一进入队列，发送就结束。每个对端由一个任务把队列写进该对端的连接，在途的数据由内核的套接字缓冲区保存。

<Diagram name="queues" words={words} />

PUSH 发送选出下一个轮到的对端，把消息排入它的队列。只有队列满了，发送才会等待，因此单靠客户端，发送方可以领先读得慢的对端 10 万条消息。本 crate 的队列发布者远在此之前就会等待：每发送 256 条消息或 1 MiB 等待一次，见[背压](../backpressure.md)一页的说明。

PULL 套接字在被要求取一条消息时，轮流从每个已连接的对端读取一条。在此之前，帧留在内核的缓冲区里，本端和发送端都有。

PUB 发送为每个过滤器匹配的订阅者排入一份消息副本，并且从不等待：队列已满、放不下的副本会被丢弃。本 crate 的广播发布者用同样的窗口约束自己，因此读得慢的订阅者会拖住它，而不是丢失消息。不匹配任何过滤器的消息会被丢弃，不报告错误。

libzmq 是其他大多数语言使用的 C 库，它同样为每个连接保存一个队列，默认 1000 条消息（高水位 `SNDHWM` 和 `RCVHWM`）。队列满时，PUB 丢弃消息，PUSH 阻塞发送。`zeromq` 客户端的队列容纳 10 万条消息，并且不可配置：套接字的选项只有对端身份、连接超时和接收缓冲区。

限制本 crate 的订阅预读多远的，是 crate 自己的通道，深度为 [`read_ahead`](../../components/subscriptions/read-ahead.md)，[背压](../backpressure.md)一页画出了它。

libzmq 的数字来自 [zguide](https://zguide.zeromq.org/docs/chapter2/)（「High-Water Marks」一节）。客户端的行为依据 [`zeromq` 0.7](https://docs.rs/zeromq/0.7.0/zeromq/) 的源码。
