---
title: 队列
sidebar_position: 2
description: "zeromq 客户端不保存消息队列：一次发送把帧写进对端的连接，在途的数据由内核的套接字缓冲区保存。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "在两个 zeromq 套接字之间，消息在哪里等待：PUSH/PULL 与 PUB/SUB",
  "text": "以文字阅读本图",
  "pushPull": "PUSH/PULL",
  "pubSub": "PUB/SUB",
  "pushsend": ["PUSH 发送", "选出轮到的", "那个对端"],
  "pushwrite": ["把帧写进", "该对端的", "连接"],
  "pushkernel": ["内核的", "套接字缓冲区，", "两端都有"],
  "pushrecv": ["PULL 接收", "轮流读取各对端", "的连接"],
  "pubsend": ["PUB 发送", "发给过滤器匹配的", "每个订阅者"],
  "pubwrite": ["逐一写进", "每个订阅者的", "连接"],
  "pubkernel": ["内核的", "套接字缓冲区，", "两端都有"],
  "pubrecv": ["SUB 接收", "读取过滤器", "放行的消息"],
  "client": ["zeromq 客户端不保存消息队列：", "帧写完，一次发送就结束"],
  "libzmq": ["libzmq 则为每个连接保存 1000 条消息", "（SNDHWM、RCVHWM）：到达高水位时 PUB 丢弃，PUSH 阻塞"],
  "inTurn": "轮流"
};

# 队列

`zeromq` 客户端不保存消息队列：一次发送把帧写进对端的连接，在途的数据由内核的套接字缓冲区保存。

<Diagram name="queues" words={words} />

PUSH 发送选出下一个轮到的对端，把帧写进该对端的连接。帧写完，发送才结束，因此读得慢的对端会让发送等待。

PULL 套接字在被要求取一条消息时，轮流从每个已连接的对端读取一条。在此之前，帧留在内核的缓冲区里，本端和发送端都有。

PUB 发送把消息逐一写进每个过滤器匹配的订阅者的连接。因此最慢的那个匹配订阅者会拖住发布者，但慢的订阅者也不会丢失消息。不匹配任何过滤器的消息会被丢弃，不报告错误。

libzmq 是其他大多数语言使用的 C 库，它的做法不同。在那里每个连接有自己的队列，默认 1000 条消息（高水位 `SNDHWM` 和 `RCVHWM`）。队列满时，PUB 丢弃消息，PUSH 阻塞发送。`zeromq` 客户端没有这样的队列，也没有这样的设置：套接字的选项只有对端身份和连接超时。

限制本 crate 的订阅预读多远的，是 crate 自己的通道，深度为 [`read_ahead`](../../components/subscriptions/read-ahead.md)，[背压](../backpressure.md)一页画出了它。

libzmq 的数字来自 [zguide](https://zguide.zeromq.org/docs/chapter2/)（「High-Water Marks」一节）。客户端的行为依据 [`zeromq` 0.6](https://docs.rs/zeromq/0.6.0/zeromq/) 的源码。
