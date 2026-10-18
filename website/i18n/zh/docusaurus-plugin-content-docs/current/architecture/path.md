---
title: 路径
sidebar_position: 4
description: "一条 ZeroMQ 消息穿过 RustStream 服务的路径：套接字任务、通道、处理器、回复的帧和发送。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条得到回复的 ZeroMQ 消息的路径，从对端的帧到回复的帧",
  "text": "以文字阅读本图",
  "peer": ["对端", "发送三帧"],
  "driver": ["套接字任务", "拥有套接字，读取一条消息"],
  "read": ["ZmqMessage", "名称、头部和载荷帧"],
  "channel": ["通道", "深度为 read_ahead"],
  "dispatch": ["分发", "一次一条，或一批"],
  "decode": ["解码", "直接从载荷帧原地读取"],
  "body": ["订阅者处理器", "#[subscriber] 下的 async fn"],
  "encode": ["回复", "编码进它交出的缓冲区"],
  "frames": ["帧", "缓冲区成为载荷帧"],
  "publisher": ["发布者", "发送，对端接入期间重试"],
  "settle": ["结算", "ack 回答 Unsupported"]
};

# 路径

一条 ZeroMQ 消息穿过 RustStream 服务的路径：套接字任务、通道、处理器、回复的帧和发送。

<Diagram name="path" words={words} />

拥有订阅套接字的任务一次接收一条消息。它把帧拆成名称、头部和载荷。载荷就是收到的那一帧本身，而不是副本，处理器的输入直接从它原地解码。

消息进入订阅的通道，分发循环从那里取出它。[批处理器](../components/subscriptions/batches.md)拿到通道里已有的消息，数量最多到挂载点指定的大小，或者是第一条消息之后 20 毫秒内到达的消息。

处理器的回复被编码进一个由发布者接管的缓冲区。这个缓冲区成为载荷帧，名称和头部各占一帧放在它前面，因此载荷不被复制。[发布者](../components/publishing/index.md)在第一次发布时接入自己的套接字。还找不到已连接对端的队列发送每 50 毫秒重试一次，最多五秒，之后失败，错误中写明目的地。

投递在回复之后结算。ZeroMQ 没有可结算的东西，因此 ack 立即回答 `Unsupported`，见[结算](settlement.md)一页的图。一条消息在这条路径上的开销（指令数和内存分配）见[基准测试](benchmarks/index.md)。
