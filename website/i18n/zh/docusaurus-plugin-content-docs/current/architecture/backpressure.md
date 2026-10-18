---
title: 背压
sidebar_position: 5
description: "订阅读取套接字时，最多领先处理器 read_ahead 条消息；超出就停止读取，等待一直传回对端的发送。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "订阅的预读：消息沿左列向下走，通道满时的等待沿右列向上传回对端",
  "text": "以文字阅读本图",
  "peer": ["对端", "PUSH 或 PUB 发送"],
  "kernel": ["内核的", "套接字缓冲区"],
  "driver": ["套接字任务", "读取一条消息"],
  "channel": ["通道", "深度为 read_ahead"],
  "handler": ["处理器", "或 workers(n) 工作分区"],
  "full": ["处理器落后：", "通道已满"],
  "stops": ["任务等待，", "停止读取"],
  "fills": ["缓冲区填满，", "TCP 窗口关闭"],
  "waits": ["对端的发送", "等待空间"],
  "pub": ["在 PUB/SUB 上，PUB 等待最慢的匹配订阅者；", "不匹配任何过滤器的消息被丢弃"]
};

# 背压

订阅读取套接字时，最多领先处理器 `read_ahead` 条消息；超出就停止读取，等待一直传回对端的发送。

<Diagram name="backpressure" words={words} />

订阅的任务从套接字上一次读一条消息，放进通道。通道容纳 [`read_ahead`](../components/subscriptions/read-ahead.md) 条消息，默认 1000 条，即 libzmq 给套接字的接收高水位。这个设置由 Broker 携带，因此该 Broker 的每个订阅都预读这么远。

处理器落后时，通道被填满，任务等待空间。等待中的任务不读取任何东西，因此帧留在内核的缓冲区里，直到缓冲区填满。随后 TCP 让发送方停下，对端的发送一直等到处理器赶上来。慢的处理器会让发送方放慢，本进程的内存保持有界。

在 PUB/SUB 上，等待的发送方是 PUB 套接字。它依次写给每个匹配的订阅者，因此一个慢订阅者会拖住发给所有订阅者的消息。不匹配任何订阅者过滤器的消息仍然被丢弃，见[队列](zeromq/queues.md)一页的说明。

除非挂载点要求更多，处理器一次只取一条消息。`workers(n)` 并排运行 `n` 个处理器，空闲的工作者从通道取下一条消息。ZeroMQ 消息不带键，因此 `workers(n, by_key)` 没有可以哈希的东西，消息在各个工作分区之间轮转。[批](../components/subscriptions/batches.md)在达到挂载点指定的大小时结束，或者在第一条消息之后 20 毫秒结束。

对端读得慢时，发布也以同样的方式等待。队列发布在等待时可以取消，下一次发布会完成它的发送。广播发布、回复和请求不能这样取消，因此[通过超时放弃它们](../components/publishing/cancellation.md)。
