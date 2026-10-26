---
title: 其他语言的对端
sidebar_position: 6
description: "加入 Python、C 或 C++ 对端已在使用的 ZeroMQ 拓扑：三个帧，PUSH、SUB 和 DEALER 对端，编解码器与版本。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 其他语言的对端

本指南把一个 RustStream 服务连接到用其他语言编写的对端：一个向它推送任务的 PUSH，一个读取它的通告的 SUB，以及一个向它发出请求的 DEALER。

这里的对端使用 [pyzmq](https://pyzmq.readthedocs.io/)，即 libzmq 的 Python 绑定。
用 C、C++ 或其他语言编写的对端，用自己的绑定组成同样的帧。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/interop.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/interop.rs#service"
```

</TabItem>
</Tabs>

服务绑定三个端点。
端口 5555 上的 `jobs` 是一个 [`ZmqQueue`](../components/broker/zmq-queue.md)，接收 PUSH 发送的任务。
每个处理完的任务都以 `jobs.done` 的名称，在端口 5556 的 [`ZmqFanout`](../components/broker/zmq-fanout.md) 上通告。
端口 5557 上的 `score` 在 [`ZmqRpc`](../components/broker/zmq-rpc.md) 上应答请求；`ReplyToRequester` 就是[请求与回复](request-reply.md)中的那个转换。

## 三个帧 {/* #the-three-frames */}

每条消息由三帧组成（[线上格式](../components/wire/index.md)）：

```text
frame 0: name      UTF-8; on PUB/SUB also what a subscriber's prefix is matched against
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   what the codec wrote; JSON by default
```

只发送名称和载荷两帧的对端，发送的是不带消息头的消息。
像普通 ZeroMQ 应用那样只发送一帧的对端，这一帧就是载荷，名称为空；在 PUB/SUB 上，前缀与这一帧匹配。
消息头帧中的空行会被跳过，因此行尾多一个换行符也没有关系。
如果消息头帧或名称不是 UTF-8，或者某个消息头行没有 `:`，这次投递就在订阅上以 [`ZmqError::Wire`](../components/wire/headers.md) 失败。
服务会检查它发布的消息头，保证每个消息头都能原样读回。
如果消息头不是文本、含有换行、名称中带有 `:`，或者以空白开头或结尾，发布就以 `ZmqError::Send` 失败，错误中写明该消息头和目的地。

## 向服务推送的 PUSH {/* #a-push-that-feeds-the-service */}

```python title="push.py"
import json
import zmq

push = zmq.Context().socket(zmq.PUSH)
push.connect("tcp://127.0.0.1:5555")
push.send_multipart([
    b"jobs",
    b"content-type: application/json",
    json.dumps({"id": 1, "items": 3}).encode(),
])
push.close(linger=1000)
```

名称帧是 `jobs`，但已绑定的 PULL 会接收推送给它的每条消息，不管名称是什么。

## 读取通告的 SUB {/* #a-sub-that-reads-the-announcements */}

```python title="watch.py"
import json
import zmq

sub = zmq.Context().socket(zmq.SUB)
sub.connect("tcp://127.0.0.1:5556")
sub.setsockopt(zmq.SUBSCRIBE, b"jobs.")
name, headers, payload = sub.recv_multipart()
print(name.decode(), json.loads(payload))
```

订阅是名称帧的前缀，这一点与 RustStream 观察者相同（[基于 PUB/SUB 的广播](broadcast.md)）。
SUB 收到它加入广播之后发布的通告，之前的一概收不到。
先启动 `watch.py`，运行 `push.py`，一秒后再运行一次，这样就有一条通告在 SUB 加入之后发布。
`watch.py` 打印它收到的第一条通告后退出：`jobs.done {'id': 1, 'items': 3}`。

## 向服务发出请求的 DEALER {/* #a-dealer-that-asks-the-service */}

```python title="ask.py"
import json
import zmq

dealer = zmq.Context().socket(zmq.DEALER)
dealer.connect("tcp://127.0.0.1:5557")
dealer.send_multipart([
    b"score",
    b"correlation-id: 42",
    json.dumps({"customer": 3, "amount_cents": 1250}).encode(),
])
name, headers, payload = dealer.recv_multipart()
print(name, headers, json.loads(payload))
```

`ZmqRpc` 另一侧的请求方是一个 DEALER，它发送这三帧。
ROUTER 在它们前面加上 DEALER 的身份，并给请求加上 `reply-to` 消息头。
答案以三帧返回，第 0 帧是字面值 `reply`（[信封](../components/wire/envelope.md)）。
消息头带着请求的 `correlation-id`，因此同时有多个请求在途的 DEALER，能把每个答案与对应的请求匹配起来：

```text
b'reply' b'correlation-id: 42' {'customer': 3, 'approved': True}
```

## 编解码器 {/* #codecs */}

载荷帧就是编解码器写出的内容，因此对端与服务约定的是编解码器，而不是 Rust 类型。
默认是 JSON；`msgpack` 和 `cbor` 是 `ruststream` 的 feature。
对端自己组帧的字节，放在 `#[derive(Outgoing, Serialized)]` newtype 中原样通过（[线上格式](../components/wire/index.md)）。

## 版本与安全 {/* #versions-and-security */}

客户端以 ZMTP 3.0 问候每个对端。
libzmq 4 使用 ZMTP 3.1，并接受 3.0 的对端；`zeromq` 客户端针对 libzmq 做过测试。
握手使用 NULL 机制，因此对端无需凭据即可加入。
流量是明文：[在生产环境中运行](production.md)讲解了应在什么环境中运行它。

## 生成文档中的约定 {/* #the-contract-in-the-generated-document */}

启用 `asyncapi` feature 后，生成的文档告诉对端如何加入每个通道（[AsyncAPI](../components/asyncapi/index.md)）。
服务器条目报告传输方式、地址和本服务所在的一端。
通道报告它的套接字对，在 PUSH/PULL 和 PUB/SUB 上还报告 `nameFrame`：对端发送或订阅时所用的第 0 帧。
应答方的回复通道指向 `reply-to` 消息头，而不是一个地址。

## 测试帧 {/* #testing-the-frames */}

进程内测试像对端那样推送一个任务，并检查发布在 `jobs.done` 上的通告。
第二个测试用 `TestApp::start_live` 在真实套接字上运行服务。
一个原始的 `zeromq` DEALER 扮演外部对端：它手工组成请求帧，并检查答案的三帧。
应答方绑定一个测试自己的套接字路径，因此并行运行的测试互不干扰。
