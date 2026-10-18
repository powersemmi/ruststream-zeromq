---
slug: /
sidebar_position: 0
description: "Руководства по сервисам на ZeroMQ с ruststream-zeromq, брокером ZeroMQ для RustStream."
---

# Руководства

Крейт `ruststream-zeromq` реализует брокер ZeroMQ для [RustStream](https://powersemmi.github.io/ruststream/latest/ru/).
Он построен на клиенте [zeromq](https://docs.rs/zeromq), целиком написанном на Rust. У ZeroMQ нет
сервера посередине: сервисы обмениваются сообщениями напрямую через сокеты `tcp://` и `ipc://`.

Для каждого паттерна сокетов крейт предлагает свой брокер. `ZmqQueue` работает по схеме PUSH/PULL:
конкурирующие потребители делят работу между собой. `ZmqFanout` работает по схеме PUB/SUB: каждый
подписчик получает свою копию. `ZmqRpc` работает по схеме DEALER/ROUTER и передаёт запрос и ответ
на него.
