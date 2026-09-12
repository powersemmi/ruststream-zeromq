# ZeroMQ

`ruststream-zeromq` 把 RustStream 服务接进一套 ZeroMQ 拓扑，底层是纯 Rust 的
[`zeromq`](https://docs.rs/zeromq) 实现，走 TCP 和 IPC 传输。中间没有服务器：两个进程直接对话。
帧布局是这个 crate 公开契约的一部分，因此两端里的任何一端都可以由非 Rust 的对端来担任。框架概念
（编写订阅者、路由、编解码器和中间件）参见 [RustStream 文档](https://powersemmi.github.io/ruststream/)。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
```

## 能力 { #capabilities }

这个传输实现了框架的哪些可选能力 trait：

| 能力 | 原生 | 原因 |
| --- | --- | --- |
| `Subscribe` | 是 | 三种已连接形态都按名字订阅。参见[三种模式](#the-three-patterns)。 |
| 确认（`ack` / `nack`） | 否 | 两者都返回 `AckError::Unsupported`。投递是**至多一次**的：套接字把消息交出去之后，没有协议帧来结算它，也没有可供重新投递的存储。延迟重试改由框架的延后副本来完成。参见[重试](#retries)。 |
| 单条消息的设置 | 无 | 一次发送只带那几个帧，别无他物。因此每个发布者都声明 `Options = ()`，这个 crate 也不往发布构建器里加任何步骤。参见[发布](#publishing)。 |
| `BatchSubscriber` | 客户端侧，在两种单向模式上 | 套接字没有批量接收，因此 `ZmqQueue` 和 `ZmqFanout` 在客户端侧按挂载点给出的大小攒批次。`ZmqRpc` 完全不实现它，这是有意为之：响应方上的 `.batch(..)` 无法通过编译。参见[批次](#batches)。 |
| `TransactionalPublisher` | 否 | ZeroMQ 没有事务。 |
| `OwnedTransactions` | 否 | ZeroMQ 没有事务。 |
| `RequestReply` | 是，在 `ZmqRpc` 上 | `ZmqRpcPublisher` 在 DEALER/ROUTER 之上实现了它，按 `correlation-id` 消息头匹配应答。`ZmqQueue` 和 `ZmqFanout` 是单向模式，没有回程，因此它们的发布者不实现它。参见[请求与响应](#request-and-reply)。 |
| `Partitioned` | 否 | Broker 侧没有分区。PUSH/PULL 在已接上的对端之间轮流分发，不看键。 |
| `Seekable` / `Positioned` | 否 | 什么都不存，因此也没有可以回到的位置。 |
| `DescribeServer` | 是 | 每种模式都报出客户端连接的地址（`tcp://` 上是 `broker:5555`，`ipc://` 上是套接字路径）和 `zeromq` 协议，这些正是 AsyncAPI schema 记录的内容。 |

## 范围 { #scope }

- 投递是至多一次的，而且什么都不存。处理器要求延迟重试时，只有框架的延后副本能满足它，响应方则完全
  无法满足（[重试](#retries)）。
- 广播模式的订阅者如果在发布者启动之后才接上，就收不到它到达之前发出的消息（慢加入者）。
- 没有高水位设置。读得慢的一端靠 TCP 本身对发送方形成背压；广播出去而没有任何订阅者匹配的消息，被
  丢掉，也不报错。
- 没有加密层，因此要把服务跑在可信网络里，或者放进已有的隧道。

## 三种模式 { #the-three-patterns }

每种模式都是一个独立的 Broker：一个已连接形态、一个构造出活的发布者的发布策略，以及它自己的订阅者。

| Broker | 套接字 | 消息形态 | 发布策略 |
| --- | --- | --- | --- |
| `ZmqQueue` | PUSH/PULL | 竞争消费者，轮流分发：每条消息只到一个消费者。 | `ZmqQueuePublish` |
| `ZmqFanout` | PUB/SUB | 广播：每条消息到达名字前缀匹配的每个订阅者。 | `ZmqFanoutPublish` |
| `ZmqRpc` | DEALER/ROUTER | 请求与响应。 | `ZmqRpcPublish` |

挂载点点名其中一个策略：`.out(Reply, policy)` 发布带 `#[subscriber(.., publish)]` 的处理器返回的
值，`.out(marker, policy)` 对注入的 `Out<..>` 发布者做同样的事。每种模式的策略同时也是它已连接形态
的默认值，因此不写 `.out` 挂载的处理器，回复照样经由它发布。

订阅有名字，这个名字就是第一个帧。`ZmqFanout` 按它过滤：名字就是订阅前缀，因此订阅 `events` 的一方
也会收到 `events.created`。`ZmqQueue` 和 `ZmqRpc` 把套接字收到的每条消息都交给订阅，不管第一个帧写
的是什么。因此，同一个端点上的两个 `ZmqQueue` 订阅会把一股工作分着做，另一种工作需要它自己的端点。

每种模式都自带 prelude，路由文件只需要这一个导入：`ruststream_zeromq::queue::prelude::*`、
`fanout::prelude::*` 或 `rpc::prelude::*`。它重导出框架的 prelude、端点、这种模式的 Broker 类型，
以及这种模式的发布策略（名字统一为 `Publish`）；`rpc::prelude` 还加上 `RequestReply`。每种模式都
把自己的策略叫 `Publish`，因此服务在模式之间搬家时，改的是导入那一行，挂载点一个字都不用动。

两套词汇，两个文件。挂载点点名策略；处理器用 Broker 的能力 trait（`Publisher`、`RequestReply`）
约束注入进来的发布者，并且只导入 `ruststream::prelude::*`。

挂载不止一种模式的文件，改为导入 `ruststream_zeromq::prelude::*`。这个 glob 重导出三个 Broker
类型，以及带前缀名字的三个策略：`ZmqQueuePublish`、`ZmqFanoutPublish` 和 `ZmqRpcPublish`。

队列模式上的一个工作进程，它只导入这种模式的 prelude：

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:handler"
```

挂载点点名模式，并把处理器包含进来：

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:app"
```

## 批次 { #batches }

接收切片的处理器拿到一整批任务，批次大小由挂载点封顶：

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_batches.rs:handler"
```

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_batches.rs:app"
```

套接字上的一次接收产出一条多部分消息，因此没有可以接受这个大小的批量接收。投递改在客户端侧攒起来：
批次攒够挂载点给出的大小就关闭，或者在第一条投递之后 20 ms 关闭，以先到者为准。这 20 ms 是固定的，
不是一项设置。

`ZmqRpc` 是例外：响应方注册上的 `.batch(..)` 无法通过编译。响应方按每条请求自己的 `reply-to` 消息
头里的地址作答，而一个批次的全部回复共用一个发布上下文。因此，回复一个批次会把每条回复都发给同一个
对端。`ZmqRpcSubscriber` 没有实现 `BatchSubscriber`，编译错误会点出它。

## 端点 { #endpoints }

中间没有服务器，因此哪一端监听是部署决定，不是传输的属性。`ZmqEndpoint` 是一个地址加上一个明确的
角色：

- `ZmqEndpoint::bind("tcp://0.0.0.0:5555")` - 本进程监听。
- `ZmqEndpoint::connect("tcp://ml:5555")` - 本进程主动拨出。
- `ZmqEndpoint::bind("ipc:///tmp/orders")` - 同一台主机，不走网络栈。

`ZmqEndpoint` 服务于 `tcp://` 和 `ipc://` 两种传输。在其他地址上连接一种模式，会在打开任何套接字
之前返回错误，消息里带上那个地址。

角色与消息的流向无关。`ZmqQueue` 的消费者可以绑定，让生产者拨进来；也可以主动拨到一个绑定的生产者
上。模式决定谁接收，端点决定谁监听。

### 临时端口绑定 { #ephemeral-binds }

端口写成零的地址（`tcp://127.0.0.1:0`）把端口交给操作系统。订阅绑定它的套接字时端口才定下来，已连接
形态上的 `bound_address()` 返回具体地址，在那之前返回 `None`。

同一进程里的发布者会自己拨到这个定下来的地址。因此，两端都在自己手里的服务（响应方和请求方在同一个
二进制里）不需要固定端口。下面请求与响应的示例就靠它。

## 生命周期 { #the-lifecycle }

每种模式都是一条消费自身的转移阶梯，每个状态都是不同的类型：

```text
ZmqQueue::new(endpoint)     只是配置，同步，无 I/O
  .connect()   ->  ConnectedZmqQueue   套接字按订阅和发布者惰性接上
  .shutdown()             ->           终态见证；先前发出的句柄会触发关闭标志
```

ZeroMQ 服务由同步的 `#[ruststream::app]` 构建器组装。`shutdown` 消费掉已连接形态，因此在它之后发布
或订阅无法通过编译。先前发出的发布者共用同一份状态，因此关闭之后它返回 `ZmqError::NotConnected`。

一次发送会在 ZMTP 握手落定的过程中重试五秒：还没有对端接上的套接字，会把消息原样退回。整个窗口里都
没找到对端的发送返回 `ZmqError::Send`，并点出目的地。

## 传输契约 { #the-wire-contract }

帧布局是公开的，跨版本稳定：另一端的对端手工拼出消息。

```text
帧 0: 名字    UTF-8；广播模式下同时也是订阅前缀
帧 1: 消息头  UTF-8 的 "名字: 值" 行，用 \n 分隔；可以为空
帧 2: 载荷    由框架的编解码器编码
```

在 `ZmqRpc` 上，回复的分帧不一样：帧 0 里放的是字面量 `reply`，它前面还有一个 ROUTER 身份帧，寻址
发问的那个对端。

Python 对端这样把工作推给 `ZmqQueue` 消费者：

```python
socket.send_multipart([b"jobs", b"content-type: application/json", payload])
```

消息头是文本。不带消息头的消息把消息头帧留空；来自最简对端的两帧消息，读作没有消息头；帧里的空行
会被跳过，因此以换行结尾的对端也能互通。

两个方向上都不做猜测。发布一个不是 UTF-8 的消息头值，返回错误并点出那个消息头，因为文本帧装不下
它。不是 UTF-8 的消息头帧返回传输错误，名字和值之间没有 `:` 的行同样如此。不是 UTF-8 的名字帧也
返回传输错误。

载荷帧里就是框架的编解码器产出的内容，因此对端只需要在编解码器上达成一致：用默认的 JSON 编解码器
时，`payload` 就是处理器输入类型反序列化所依据的那份 JSON 文档。

## 发布 { #publishing }

发布走框架自己的发布构建器，这个传输不加自己的步骤。`message(..)` 接受值；目的地、消息头和编解码
器，从点名了它们的最具体那一层解析出来。框架自己的发布指南在这里原样适用。

服务手里已经编码好的字节是这里的常见情形，因为另一端的对端已经把它们分好帧了。把它们包进一个
`#[derive(Outgoing, Serialized)]` 的 newtype，再用同一个 `message(..)` 调用发布出去。编解码器不会
作用在它们身上，而这个类型给本来匿名的载荷起了名字。

一次发送只带那几个帧，别无他物：没有优先级、没有过期时间、也没有排序键。因此这里每个发布者都声明
`Options = ()`，没有可以按条消息调整的设置。在有这类设置的 Broker 上，处理器主体要用那一步时，导入
该 Broker 的 prelude，并在约束里点名它的 options 类型；在这里，处理器文件只导入
`ruststream::prelude::*`，不论它跑在哪种模式上。

## 重试 { #retries }

投递从不结算，因此返回 `HandlerOutcome::retry_after(..)` 的处理器只剩一条路：延迟过去之后，运行时
经由作用域用 `retry_via` 接上的发布者发布一份副本。在作用域上接一个，副本就发往订阅报出的地址。

`ZmqQueue` 和 `ZmqFanout` 报出自己订阅的名字，同一个 Broker 上的发布者正是按它够到它们。队列上的
重试回到队列里，哪个工作进程空着就归哪个；广播上的重试到达前缀匹配的每一条订阅，也就是原件当初的
同一批听众。

`ZmqRpc` 什么都不报：响应方的名字不是发布目的地，回复按请求带来的对端身份路由。在响应方上接了
`retry_via` 的作用域启动时会被拒绝，错误里点出那条订阅。让请求方再问一次就是了。

## 请求与响应 { #request-and-reply }

`ZmqRpc` 覆盖 DEALER/ROUTER 的两端。

请求方这一侧用 `ZmqRpcPublisher` 上的 `RequestReply` 能力。`request(msg, timeout)` 经由 DEALER
套接字发出，并返回按 `correlation-id` 消息头匹配到的应答。超时之内没人应答时，它改为返回一个超时
错误。关联 id 你可以自己写在请求上，传输会原样保留，因此上层可以按自己的标识来匹配。

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:request"
```

响应方这一侧就是一个普通的回复处理器。ROUTER 套接字给每条请求加上一个 `reply-to` 消息头，指向发出
请求的那个对端，再由一个发布变换把回复的目的地改写成这个地址。应答是按请求逐条寻址的，因此它的类型
不声明自己的目的地，`publish("..")` 子句里的名字只是兜底：生成的文档报的是它，变换没有动过的那次
投递也答到它那里。

写入目的地的变换声明 `Destination = Names`，而只有目的地尚未被声明的位置才给出这项权利。因此，自己
点名目的地的回复类型和写入目的地的变换无法一起通过编译，文档也就不会承诺一个通道、传输上却走另一
个：

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:transform"
```

挂载点在回复位置点名这个策略，并把变换挂上去：

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_request_reply.rs:responder"
```

可以直接运行的程序是
[`examples/zmq_request_reply.rs`](https://github.com/powersemmi/ruststream-zeromq/blob/main/crates/ruststream-zeromq/examples/zmq_request_reply.rs)：
响应方和请求方在同一个进程里，走一次临时端口绑定。

## 测试 { #testing }

`testing` feature 提供 `ZmqTestBroker`：一个进程内传输，不用套接字、不走网络就复现这个 crate 的
路由。它遵循与真实模式相同的那条阶梯。它一次投递一条消息，并像 `ZmqQueue` 那样在客户端侧攒
批次，因此在生产中跑得起来的批量处理器，在测试套件下也跑得起来。

用 `TestApp` 测试套件来驱动它。`TestApp::start(app).await?` 在进程内连接应用的各个 Broker，并返回
已启动的测试套件，也就是下面的 `tb`；它上面的 `tb.broker::<ZmqTestBroker>()` 就是这个传输的句柄。
从这个句柄出发，`.message(&job).to("jobs").publish()` 送进一个任务，
`.subscriber("jobs").assert_called_once().with(&job)` 断言处理器收到了什么，
`.published::<Done>("results").assert_called_once().with(&done)` 断言发布型处理器发出了什么。参见
[用 TestApp 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

### 挂载点保留自己的策略 { #mount-sites-keep-their-policy }

三个生产策略在这个进程内传输上同样能构造出发布者，因此测试套件下的路由文件就是服务实际发布的那一
份：`ZmqQueuePublish`、`ZmqFanoutPublish` 和 `ZmqRpcPublish` 都能挂到 `ZmqTestBroker` 的挂载点上。
绑定 `Out<impl RequestReply, ..>` 的处理器在这里挂到 rpc 策略上，和它在套接字之上的做法一模一样；
并且和生产中一样，只能挂到这一个策略上。

一个进程内传输覆盖三种模式，但一个 Broker 只点名一个默认发布策略。因此，省略 `.out(Reply, ..)` 的
挂载在这里一律走队列规则，不论服务实际跑在哪种模式上。在挂载点点名策略
（`.out(Reply, ZmqFanoutPublish)`），测试套件和部署就会以同样的方式发布。

### 每种模式在进程内保留了什么 { #what-each-pattern-keeps-in-process }

挂载点点名的策略带着这种模式的投递规则，因此三者之间的差别一直延续到测试里：

- **队列。** 每条消息到达目的地上的其中一个消费者，消费者轮流拿。挂在同一个名字上的两个工作进程
  会把工作分开，而不是两个都跑一遍。
- **广播。** 每条消息到达名字是目的地前缀的每一条订阅，这是协议自己的过滤规则，因此订阅 `orders`
  能看到 `orders.eu.1`；没有任何订阅匹配的消息被丢掉，发布本身不报错。
- **请求-响应。** 一条请求带着 `reply-to` 回复地址和 `correlation-id`；应答路由回发问的那个调用
  方，而且只有回显了那个 id 的应答才能了结这次请求。发布到回复地址以外的回复会被拒绝，和套接字
  发布者给出的拒绝一样。因此，没挂回复路由变换的响应方在这里就出错，而不是等到部署之后。

活得比连接更久的句柄会说出来。`shutdown` 先关闭传输，再丢掉它携带的东西，因此先前配好的发布者、
或者 Broker 的一个副本，报的是 `ZmqError::NotConnected`，而不是往一个已经不在的 Broker 里发布。
这正是真实发布者给出的答案，也是服务会去匹配的那个。

### 它不复现什么 { #what-it-does-not-reproduce }

凡是需要对端的，它都不复现，因为进程内的通道没有对端。真实的 PUSH 套接字在没有任何东西连上来时会
阻塞、然后返回错误，而这里的一次发布是记录下来再丢掉：“已连接”指的是另一个进程里的套接字，进程
内没有与之对应的东西。请求超时只覆盖等待应答这一段，从不涉及够到对端。投递保证、高水位和慢加入
者，自始至终都是传输层的行为。

结算是照实复现，而不是放宽。ZeroMQ 什么都不确认，因此进程内的一次投递对 `ack` 和 `nack` 都返回
`AckError::Unsupported`，并且再也不回来，与经由套接字的投递完全一样。靠重试来结算的处理器在这里
只被调用一次，它在部署之后运行的次数也是这么多；重新投递要拿有这项能力的 Broker 来覆盖。

有两处差别是反过来的，也就是进程内传输给的比真实传输多。两者都源自同一个缺口：这里的订阅按名字
给出，而名字并不说明它属于哪种模式。发布这一侧没有这个缺口，因为那里由挂载点上的策略点名模式。

**响应方的订阅者。** `ZmqRpcSubscriber` 刻意不是 `BatchSubscriber`，因此请求-响应挂载上的
`.batch(..)` 在生产中无法通过编译，而同一个挂载在测试套件下却能编译。

**重试地址。** 这里的每条订阅都报出自己的名字，也就是两种单向模式的答案，因此接了 `retry_via` 的
作用域能启动。在 `ZmqRpc` 上，同样的作用域启动时会被拒绝（[重试](#retries)）。响应方的重试接线，
要拿真实模式来验证。

套接字层面的行为同样不需要外部服务。`conformance` 的路由套件、生命周期阶梯、批次和请求/响应
这两项能力，以及由一个扮演外部对端的原始套接字驱动的帧布局检查，全都跑在回环套接字上。
因此 `just test` 覆盖整个 crate，事先不用启动任何东西。
