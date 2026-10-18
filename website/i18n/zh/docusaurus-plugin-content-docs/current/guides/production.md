---
title: 在生产环境中运行
sidebar_position: 8
description: "在生产环境中运行 ZeroMQ 服务：从环境变量读取端点、启动顺序、预读与背压、安全以及关闭。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在生产环境中运行

ZeroMQ 在服务之间没有服务器，因此原本由 Broker 决定的事情改由部署来决定：谁来监听，服务按什么顺序启动，以及慢消费者能缓冲多少。

## 来自环境变量的端点 {/* #endpoints-from-the-environment */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#settings"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#settings"
```

</TabItem>
</Tabs>

端点和预读上限就是服务的全部 ZeroMQ 配置。
`Settings` 从部署设置的环境变量中读取它们，每一项都有默认值。
`from_vars` 以函数形式接收查找操作，因此测试可以传入自己的值，而不触碰进程的环境变量。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

工作者绑定 `JOBS_BIND`，并拨号连接位于 `SINK_ADDR` 的汇集器。
`tcp://0.0.0.0:5555` 在所有网络接口上监听；`tcp://10.0.0.5:5555` 只在一个接口上监听。
`ipc:///run/jobs.sock` 是供同一主机上的服务使用的 Unix 套接字。
连接的哪一端是 Broker 类型的一部分，因此地址由部署选择，这一端由代码选择。

## 启动顺序 {/* #startup-order */}

绑定的订阅在启动时打开它的端口；端口被另一个进程占用时，启动以 [`ZmqError::Endpoint`](../components/broker/startup.md) 失败。
拨号连接的订阅等待它的对端开始监听。
客户端最多重试连接 30 秒，之后启动以 `ZmqError::Endpoint` 失败，错误中写明这个地址。
因此拨号连接的服务可以先于它所连接的对端启动，只要对端在这段时间内开始监听。

绑定一端的发布者从服务启动时就开始监听，因此拨号连接它的对端能在第一条消息之前连上。
拨号连接的发布者在第一次发布时接入。
没有已接入对端的发送会重试五秒，然后以 [`ZmqError::Send`](../components/broker/errors.md) 失败，错误中写明目的地。

## 对端重启 {/* #peer-restarts */}

拨号连接对端的发布者在对端消失后会重新拨号，对端恢复后即可再次到达。
队列发布者通过一次失败的发送得知连接丢失：发进已断开连接的消息会丢失，下一次发布返回 [`ZmqError::Send`](../components/broker/errors.md)，再下一次发布重新拨号。
广播发布者从自己的套接字得知连接丢失，并在下一次发布时重新拨号。
拨号连接的广播订阅由客户端自动重新拨号。
拨号连接 PUSH 对端的订阅保留它打开的那条连接，因此该对端重启后，它收不到任何消息，直到服务也重启。

## 预读与背压 {/* #read-ahead-and-back-pressure */}

订阅最多领先处理器 [`read_ahead`](../components/subscriptions/read-ahead.md) 次投递读取，默认 1000。
超出这个上限后它停止读取，套接字缓冲区填满，发送方随之等待。
因此慢的处理器会让发送方放慢，而不会让服务的内存增长。
在 PUB/SUB 上，等待的发送方是发布者，它被最慢的匹配订阅者拖住。
这个上限按 Broker 设置，这里取自 `ZMQ_READ_AHEAD`。

## 安全 {/* #security */}

客户端协商 NULL 机制：对端无需凭据即可连接，流量以明文字节传输。
因此服务要运行在它信任的网络中，或运行在隧道中：VPN、WireGuard 或 SSH 隧道。
生成的 AsyncAPI 文档报告每个端点的主机和端口，不带凭据（[Server](../components/asyncapi/server.md)）。

## 关闭 {/* #shutdown */}

关闭时，服务停止它的订阅，并关闭它的套接字。
此后，先前交出的发布者以 [`ZmqError::NotConnected`](../components/broker/shutdown.md) 失败，而不是向空无一物的连接发送。
队列发布可以安全地取消。
广播发布、回复和请求则不能，因此要通过超时来放弃，而不是通过 `select!` 分支（[取消](../components/publishing/cancellation.md)）。

## 测试配置 {/* #testing-the-configuration */}

测试用 `from_vars` 构建 `Settings`。
一个测试检查空环境下的默认值。
另一个测试在 `TestApp::start` 下启动配置好的服务，并检查结果到达了汇集器。
进程内模式不打开套接字，因此这些地址不必存在。
