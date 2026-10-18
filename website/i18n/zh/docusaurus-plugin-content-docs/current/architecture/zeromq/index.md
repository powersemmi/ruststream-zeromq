---
title: 套接字
sidebar_position: 0
description: "ZeroMQ 是一个套接字库，套接字之间直接通信：没有服务器，不存储任何东西，由模式决定每条消息由谁接收。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "三种套接字模式：流水线、按前缀过滤的广播、按连接区分的请求，对端之间没有服务器",
  "text": "以文字阅读本图",
  "pipeline": "PUSH/PULL：流水线",
  "ventilator": ["分发器：PUSH", "绑定"],
  "worker1": ["工作者 1", "PULL，拨号连接", "收到 1 和 4"],
  "worker2": ["工作者 2", "PULL，拨号连接", "收到 2"],
  "worker3": ["工作者 3", "PULL，拨号连接", "收到 3"],
  "sink": ["汇集器：PULL，绑定", "轮流读取"],
  "dealt": "轮流",
  "pubsub": "PUB/SUB：广播",
  "pub": ["发布者：PUB，绑定", "匹配过滤器"],
  "jobs": ["SUB，拨号连接", "过滤器 jobs."],
  "prices": ["SUB，拨号连接", "过滤器 prices"],
  "unmatched": ["orders.new：", "没有过滤器匹配，", "被丢弃"],
  "jobsDone": "jobs.done",
  "pricesEur": "prices.eur",
  "reqrep": "DEALER/ROUTER：请求与回复",
  "router": ["应答方：ROUTER，绑定", "每个连接一个身份"],
  "dealerA": ["请求方：DEALER", "拨号连接"],
  "dealerB": ["请求方：DEALER", "拨号连接"],
  "identityA": "身份 A",
  "identityB": "身份 B"
};

# 套接字

ZeroMQ 是一个套接字库，套接字之间直接通信：没有服务器，不存储任何东西，由模式决定每条消息由谁接收。

<Diagram name="sockets" words={words} />

图中的每个方框都是某个进程里的一个套接字。消息沿着两个套接字之间的连接，从一个套接字到达另一个套接字。之后没有任何东西保留它，因此当时没有连接的套接字永远看不到这条消息。

哪一端监听、哪一端拨号，与消息流动的方向无关。拓扑中稳定的部分绑定，来来去去的部分拨号连接它。在流水线中，分发器和汇集器绑定，工作者拨号连接这两者。[`ZmqEndpoint`](../../components/broker/endpoint/index.md) 声明本服务位于哪一端。

PUSH 套接字把消息轮流分给已连接的对端，一条消息只给一个对端。PULL 套接字轮流读取它的所有对端，因此任何对端都不会让其他对端饿死。这就是 [`ZmqQueue`](../../components/broker/zmq-queue.md)。

SUB 套接字把自己的过滤器发给它所连接的 PUB。PUB 用每条消息的第一帧按前缀与每个过滤器比较，只发往有过滤器匹配的地方。没有任何过滤器匹配的消息会被丢弃，发布者不会得到通知。这就是 [`ZmqFanout`](../../components/broker/zmq-fanout.md)，其中订阅的名称就是它的[过滤器](../../components/subscriptions/prefixes.md)。

ROUTER（路由器）套接字给每个连接一个身份。它把身份放在收到的每条消息前面，并把消息发给身份等于第一帧的那个连接。DEALER 收发消息时不带身份帧。这就是 [`ZmqRpc`](../../components/broker/zmq-rpc.md)，[帧](frames.md)一页画出了这个信封。

一个套接字可以有多个对端，顺序只在一对套接字之间成立。`ruststream-zeromq` 运行在 `tcp://` 和 `ipc://` 上。其他协议方案会在 Broker 连接时被拒绝，此时还没有打开任何套接字。

这些模式是 ZeroMQ 自己的（[流水线](https://rfc.zeromq.org/spec/30/)、[发布-订阅](https://rfc.zeromq.org/spec/29/)、[请求-回复](https://rfc.zeromq.org/spec/28/)），[zguide](https://zguide.zeromq.org/docs/chapter2/) 说明了哪一端绑定。
