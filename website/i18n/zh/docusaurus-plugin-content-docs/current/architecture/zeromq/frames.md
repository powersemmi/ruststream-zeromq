---
title: 帧
sidebar_position: 1
description: "ZMTP 连接以问候和 READY 命令开始，之后承载由帧组成的消息；本 crate 的一条消息是三帧：名称、消息头、载荷。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "ZMTP 连接的建立过程，以及本 crate 的一条消息由哪三帧组成",
  "text": "以文字阅读本图",
  "connection": "ZMTP 连接，双方对端",
  "greeting": ["问候", "签名、3.0 版本、NULL 机制"],
  "ready": ["READY 命令", "Socket-Type，套接字有身份时还有 Identity"],
  "messages": ["消息", "由帧组成，除最后一帧外都带 MORE 标志"],
  "message": "本 crate 的一条消息",
  "name": ["第 0 帧", "名称"],
  "headers": ["第 1 帧", "消息头"],
  "payload": ["第 2 帧", "载荷"],
  "rules": ["名称是 UTF-8 文本，消息头是 name: value 行，", "载荷是编解码器写出的字节"],
  "bothWays": "双向",
  "oneMessage": "一条消息"
};

export const envelope = {
  "title": "应答方 ROUTER 收到的请求及其回复：身份帧变成 reply-to，回复跟在它后面发出",
  "text": "以文字阅读本图",
  "request": "ROUTER 读到的请求",
  "requestIdentity": ["身份", "来自对端"],
  "requestName": ["名称"],
  "requestHeaders": ["消息头"],
  "requestPayload": ["载荷"],
  "stamp": ["处理器看到的消息头", "reply-to: zmq-reply:<十六进制身份>"],
  "reply": "ROUTER 发出的回复",
  "replyIdentity": ["身份", "来自 reply-to"],
  "replyName": ["reply"],
  "replyHeaders": ["消息头"],
  "replyPayload": ["载荷"],
  "dealer": ["ROUTER 去掉身份帧，", "请求方的 DEALER 收到其余三帧"],
  "becomes": "变成",
  "addresses": "为回复寻址"
};

# 帧

ZMTP 连接以问候和 READY 命令开始，之后承载由帧组成的消息；本 crate 的一条消息是三帧：名称、消息头、载荷。

<Diagram name="frames" words={words} />

ZMTP 是每个 ZeroMQ 套接字底下的协议。双方对端各自发送问候，其中包含协议版本和安全机制。`zeromq` 客户端以 3.0 版本和 NULL 机制问候，这种机制既不认证也不加密。随后每个对端发送一个带有自身套接字类型的 READY 命令。如果两种类型不能配对，连接就此结束。

一条消息是一组帧。除最后一帧外，每一帧都带 MORE 标志，接收方由此知道一条消息在哪里结束。套接字要么交出整条消息，要么什么也不交出。

本 crate 在每条消息中放入三帧。第 0 帧是名称，第 1 帧是[消息头](../../components/wire/headers.md)，第 2 帧是载荷。任何语言的对端都可以用一次多部分发送组成这样一条消息，见[线上格式](../../components/wire/index.md)。只有两帧的消息按名称和载荷读取，没有消息头。

载荷帧就是编解码器写出的缓冲区，而不是它的副本。本 crate 的每个发布者都接收运行时编码好的字节，而帧拥有自己的字节，因此缓冲区直接成为帧。

## 请求与回复 {/* #requests-and-replies */}

<Diagram name="envelope" words={envelope} />

应答方的 ROUTER 读到的请求，在三帧前面带着请求方的身份。本 crate 把这个身份变成 `reply-to` 消息头，即 `zmq-reply:` 后接十六进制的身份，处理器的回复就发往那里。[应答方](../../components/request-reply/responders.md)挂载一个转换，为每次投递指定这个目的地。

回程的帧依次是身份、字面名称 `reply`、消息头和载荷。ROUTER 把它们发给身份所指的连接，并去掉身份。

请求通过它自己的 DEALER 发出，带着自己的随机身份，因此一个回答只能到达提出它的那个请求。请求带有 `correlation-id` 消息头，调用方设置了它就保留原值。带有相同 id 的回复完成这个请求。[信封](../../components/wire/envelope.md)一页列出了这些消息头。

协议是 [ZMTP 3.0](https://rfc.zeromq.org/spec/23/)，身份信封来自[请求-回复模式](https://rfc.zeromq.org/spec/28/)。
