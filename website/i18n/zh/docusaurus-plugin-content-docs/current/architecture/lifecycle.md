---
title: 生命周期
sidebar_position: 3
description: "ZmqQueue::new 记录端点，connect 检查它，每个订阅和绑定一端的发布者在启动时接入套接字，拨号连接的发布者在第一次发布时接入。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 以类型表示的三种状态，以及每次转换在 ZeroMQ 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "zeromq": "在 ZeroMQ 上",
  "unconnected": ["ZmqQueue::new", "(endpoint)"],
  "connected": ["ConnectedZmqQueue"],
  "closed": ["Closed = ()"],
  "config": ["记录端点，", "不做 I/O"],
  "check": ["检查地址：", "tcp:// 或 ipc://"],
  "subscriptions": ["每个订阅", "绑定或拨号连接", "自己的套接字"],
  "publishers": ["发布者的套接字：", "绑定端在启动时接入，", "拨号端在第一次发布时接入"],
  "flag": ["标记为已关闭，", "没有要刷出的内容"],
  "afterShutdown": ["比连接活得更久的发布者回答", "ZmqError::NotConnected，绝不会静默成功"],
  "dropped": ["被丢弃的订阅停止它的任务，", "并释放它绑定的端点"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`ZmqQueue::new`](../components/broker/zmq-queue.md) 记录端点，connect 检查它，每个订阅和绑定一端的发布者在启动时接入套接字，拨号连接的发布者在第一次发布时接入。[`ZmqFanout`](../components/broker/zmq-fanout.md) 和 [`ZmqRpc`](../components/broker/zmq-rpc.md) 经历同样的步骤。

<Diagram name="lifecycle" words={words} />

`new` 不做 I/O，因此服务的构建器保持同步。运行时在应用启动时连接 Broker。connect 检查地址是 `tcp://` 还是 `ipc://`，不打开套接字。其他协议方案会让[启动](../components/broker/startup.md)失败，错误中写明这个地址。

每个订阅在打开时接入自己的套接字。在本服务[绑定](../components/broker/endpoint/bind.md)的端点上，订阅在那里监听，系统拒绝绑定就会让启动失败。一个已绑定的端点只归一个订阅：同一个 Broker 上的第二个订阅会被拒绝，错误中写明两者。在本服务[连接](../components/broker/endpoint/connect.md)的端点上，订阅拨号连接它。订阅的对端是该模式的发送端，因此从这时起，通过同一个 Broker 的发布会在拨号之前被拒绝。

只消费的服务不接入发布者套接字。绑定一端的发布者在启动时接入自己的套接字，拨号连接的发布者在第一次发布时接入。在与对端的连接完成握手之前，发送最多重试五秒。

[关闭](../components/broker/shutdown.md)把连接标记为已关闭，然后返回。本 crate 不缓冲任何东西，因此没有要刷出的内容，关闭后的形态也不携带报告。此前交出的发布者从这时起回答 [`ZmqError::NotConnected`](../components/broker/errors.md)。

运行时丢弃订阅时，订阅就停止。丢弃订阅会停止读取其套接字的任务，因为在没有对端的套接字上接收，永远不会自行返回。绑定了端点的订阅在这一刻释放端点，于是新的订阅可以绑定同一个地址。
