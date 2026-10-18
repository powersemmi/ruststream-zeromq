---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-zeromq, первый воркер на сокете PULL, задания от узла на Python и тест, который не открывает ни одного сокета."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-zeromq`, запускает первый воркер и передаёт ему задания от узла в другом процессе.

## Установка

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-zeromq = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-zeromq` - брокер ZeroMQ.
Его фича `testing` добавляет [внутрипроцессный режим](../components/testing/index.md), в котором работают тесты.

У крейта брокера такие фичи:

| Фича | Что она даёт |
|---|---|
| `asyncapi` | [расширение `x-ruststream-zeromq`](../components/asyncapi/index.md) в генерируемом документе AsyncAPI |
| `testing` | внутрипроцессный режим, в котором работает `TestApp` |

У ZeroMQ нет сервера, поэтому устанавливать и запускать ничего не нужно.
Крейт работает по протоколу ZMTP через [zeromq](https://docs.rs/zeromq), клиент на Rust, поэтому сборке не нужны ни libzmq, ни компилятор C.
Два процесса обмениваются сообщениями напрямую: через `tcp://` или через `ipc://`, сокет Unix на одном хосте.

## Первый сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_job` читает задания в JSON из `jobs`.
Задание с позициями он подтверждает, а задание без позиций отбрасывает.
[`ZmqQueue`](../components/broker/zmq-queue.md) - паттерн PUSH/PULL: каждое задание достаётся одному воркеру.
[`ZmqEndpoint::bind`](../components/broker/endpoint/bind.md) означает, что этот процесс слушает адрес.
`ZmqQueue::new` только запоминает конечную точку. Сокет открывается при [запуске](../components/broker/startup.md) сервиса.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

## Узел в другом процессе

Отправлять задания воркеру может любой процесс, который работает по ZMTP.
Сообщение состоит из трёх кадров: имени, заголовков и полезной нагрузки ([формат передачи](../components/wire/index.md)).
Этот узел на Python использует [pyzmq](https://pyzmq.readthedocs.io/):

```python title="push_job.py"
import json
import zmq

push = zmq.Context().socket(zmq.PUSH)
push.connect("tcp://127.0.0.1:5555")
push.send_multipart([b"jobs", b"", json.dumps({"id": 1, "items": 3}).encode()])
push.close(linger=1000)
```

Запустите его из другого терминала:

```bash
python3 push_job.py
```

Сервис пишет в журнал `job taken` с номером задания и числом позиций.
За этой записью идёт предупреждение `ack / nack failed`: брокер не поддерживает подтверждение.
В протоколе ZeroMQ подтверждения нет, поэтому задание считается выполненным, как только воркер его прочитал ([подтверждение](../components/settlement/index.md)).
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает сервис.
Узлом может быть и сервис на RustStream: в руководстве [Конвейеры на PUSH/PULL](pipelines.md) такой сервис отправляет свои результаты другому.

## Первый тест

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, отправляет задание и проверяет, как `take_job` завершил его доставку.
Брокер там работает внутри процесса, поэтому тесты не открывают ни одного сокета:

```bash
cargo test
```

Руководство [Тестирование сервиса на ZeroMQ](testing.md) разбирает обвязку и тот же тест на настоящих сокетах.

## Переход с pyzmq или libzmq

Сервис на pyzmq или libzmq сам открывает сокеты и пишет свой цикл приёма.
Вот куда переходит каждая часть в RustStream:

| pyzmq или libzmq | RustStream |
|---|---|
| `ctx.socket(zmq.PULL)` и `bind` | [`ZmqQueue::new(ZmqEndpoint::bind(..))`](../components/broker/zmq-queue.md) и обработчик `#[subscriber]` |
| `connect` | [`ZmqEndpoint::connect`](../components/broker/endpoint/connect.md); сторона входит в тип брокера |
| цикл `recv_multipart` | обработчик, который вызывается один раз на каждое сообщение |
| декодирование кадра полезной нагрузки | тип параметра обработчика, который декодирует кодек |
| `send_multipart` | ответ или параметр `Out`, отправленный через [политику публикации](../components/publishing/index.md) |
| `setsockopt(zmq.SUBSCRIBE, prefix)` | имя подписки на [`ZmqFanout`](../components/broker/zmq-fanout.md) ([Рассылка на PUB/SUB](broadcast.md)) |
| REQ и REP | [`ZmqRpc`](../components/broker/zmq-rpc.md) поверх DEALER и ROUTER ([Запрос и ответ](request-reply.md)) |
| `tcp://*:5555` | `tcp://0.0.0.0:5555` |
| `inproc://` | [`TestApp`](../components/testing/index.md) в тестах; `ipc://` между процессами на одном хосте |
| `SNDHWM` и `RCVHWM` | [`read_ahead`](../components/subscriptions/read-ahead.md) ([Промышленная эксплуатация](production.md)) |
| `zmq.Poller` над несколькими сокетами | по подписке на каждый сокет, каждая в своей задаче |
