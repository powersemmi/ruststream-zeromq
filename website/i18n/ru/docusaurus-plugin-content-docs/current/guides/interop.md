---
title: Узлы на других языках
sidebar_position: 6
description: "Подключение к топологии ZeroMQ, где уже работают узлы на Python, C или C++: три кадра, узлы PUSH, SUB и DEALER, кодеки и версии."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Узлы на других языках

Это руководство связывает сервис на RustStream с узлами на других языках: PUSH, который передаёт сервису задания, SUB, который читает его оповещения, и DEALER, который отправляет ему запросы.

Узлы здесь используют [pyzmq](https://pyzmq.readthedocs.io/), обёртку libzmq для Python.
Узел на C, C++ или другом языке составляет те же кадры через свою обёртку.

## Сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/interop.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/interop.rs#service"
```

</TabItem>
</Tabs>

Сервис привязывается к трём конечным точкам.
`jobs` на порту 5555 - это [`ZmqQueue`](../components/broker/zmq-queue.md), которая принимает задания от PUSH.
О каждом выполненном задании сервис оповещает как `jobs.done` через [`ZmqFanout`](../components/broker/zmq-fanout.md) на порту 5556.
`score` на порту 5557 отвечает на запросы через [`ZmqRpc`](../components/broker/zmq-rpc.md). `ReplyToRequester` - преобразование из руководства [Запрос и ответ](request-reply.md).

## Три кадра

Каждое сообщение состоит из трёх кадров ([формат передачи](../components/wire/index.md)):

```text
frame 0: name      UTF-8; on PUB/SUB also what a subscriber's prefix is matched against
frame 1: headers   UTF-8 "name: value" lines separated by \n; may be empty
frame 2: payload   what the codec wrote; JSON by default
```

Узел, который отправляет два кадра, имя и полезную нагрузку, отправляет сообщение без заголовков.
Пустая строка в кадре заголовков пропускается, поэтому перевод строки в конце строки допустим.
Если кадр заголовков или имя не в UTF-8 либо в строке заголовка нет `:`, подписка вместо доставки возвращает ошибку [`ZmqError::Wire`](../components/wire/headers.md).
Сервис проверяет заголовки, которые публикует, чтобы каждый читался обратно без изменений.
Если заголовок не текстовый, содержит перевод строки, имеет `:` в имени или начинается либо заканчивается пробельным символом, публикация возвращает ошибку `ZmqError::Send` с именем заголовка и адресом назначения.

## PUSH, который передаёт задания

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

Кадр имени здесь `jobs`, но привязанный PULL принимает каждое отправленное ему сообщение, какое бы имя в нём ни стояло.

## SUB, который читает оповещения

```python title="watch.py"
import json
import zmq

sub = zmq.Context().socket(zmq.SUB)
sub.connect("tcp://127.0.0.1:5556")
sub.setsockopt(zmq.SUBSCRIBE, b"jobs.")
name, headers, payload = sub.recv_multipart()
print(name.decode(), json.loads(payload))
```

Подписка - префикс кадра имени, как у наблюдателя на RustStream ([Рассылка на PUB/SUB](broadcast.md)).
SUB получает оповещения, опубликованные после того, как он присоединился к широковещательной конечной точке, и ни одного из более ранних.
Запустите `watch.py`, затем `push.py`, и через секунду запустите `push.py` ещё раз, чтобы оповещение было опубликовано уже после присоединения SUB.
`watch.py` печатает первое полученное оповещение и завершается: `jobs.done {'id': 1, 'items': 3}`.

## DEALER, который отправляет запросы

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

Запрашивающая сторона по другую сторону `ZmqRpc` - это DEALER, и он отправляет три кадра.
ROUTER ставит перед ними идентичность DEALER, а в запрос добавляет заголовок `reply-to`.
Ответ возвращается тремя кадрами, и кадр 0 - буквально `reply` ([конверт](../components/wire/envelope.md)).
В заголовках ответа стоит `correlation-id` запроса, поэтому DEALER с несколькими запросами в работе сопоставляет каждый ответ со своим запросом:

```text
b'reply' b'correlation-id: 42' {'customer': 3, 'approved': True}
```

## Кодеки

Кадр полезной нагрузки содержит то, что записал кодек, поэтому узел договаривается с сервисом о кодеке, а не о типах Rust.
По умолчанию это JSON. `msgpack` и `cbor` - фичи `ruststream`.
Байты, которые узел оформил сам, проходят без изменений в обёртке `#[derive(Outgoing, Serialized)]` ([формат передачи](../components/wire/index.md)).

## Версии и безопасность

Клиент приветствует каждый узел по ZMTP 3.0.
libzmq 4 работает по ZMTP 3.1 и принимает узел на 3.0, а клиент `zeromq` проверен на совместимость с libzmq.
Рукопожатие использует механизм NULL, поэтому узел присоединяется без учётных данных.
Трафик не шифруется. Где его запускать, описывает руководство [Промышленная эксплуатация](production.md).

## Контракт в генерируемом документе

С фичей `asyncapi` генерируемый документ сообщает узлу, как присоединиться к каждому каналу ([AsyncAPI](../components/asyncapi/index.md)).
Сервер сообщает транспорт, адрес и сторону, которую занимает этот сервис.
Канал сообщает свою пару сокетов, а на PUSH/PULL и PUB/SUB ещё и `nameFrame` - кадр 0, с которым узел отправляет сообщения или подписывается.
Канал ответов отвечающей стороны вместо адреса указывает на заголовок `reply-to`.

## Проверка кадров

Внутрипроцессный тест отправляет задание так, как это сделал бы узел, и проверяет оповещение, опубликованное в `jobs.done`.
Второй тест запускает сервис на настоящих сокетах через `TestApp::start_live`.
Роль чужого узла играет сокет DEALER клиента `zeromq` без фреймворка: он вручную составляет кадры запроса и проверяет три кадра ответа.
Отвечающая сторона привязывается к пути сокета, который принадлежит только этому тесту, поэтому параллельные запуски никогда не встречаются.
