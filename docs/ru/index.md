# ruststream-zeromq

**`ruststream-zeromq`** подключает сервис на [RustStream](https://powersemmi.github.io/ruststream/)
к топологии ZeroMQ через реализацию [`zeromq`](https://docs.rs/zeromq) на чистом Rust, поверх
транспортов TCP и IPC.

Сервера посередине нет: два процесса разговаривают напрямую. Формат кадров описан, поэтому узел на
другом конце собирает сообщения вручную. Сервис на Rust встраивается в топологию, где уже работают
готовый рабочий процесс на Python или демон на C++.

Три паттерна сокетов покрывают три формы обмена: `ZmqQueue` (PUSH/PULL, конкурирующие потребители),
`ZmqFanout` (PUB/SUB, широковещательная рассылка с фильтром по префиксу) и `ZmqRpc` (DEALER/ROUTER,
запрос и ответ).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-zeromq = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-zeromq/examples/zmq_pipeline.rs:app"
```

Справочник и руководство крейта теперь одна страница:
[обзор `ruststream-zeromq` на docs.rs](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html) описывает
[три паттерна](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-three-patterns), [конечные точки](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#endpoints),
[подписку](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#subscribing) с её пакетами и повторами,
[публикацию](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#publishing), [контракт передачи](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-wire-contract), по которому
собирает сообщения узел не на Rust, [порождаемый документ](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#the-generated-document),
[тестирование](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#testing) рабочего приложения во внутрипроцессном режиме или через сокеты на петлевом интерфейсе и [эксплуатацию](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html#operations).

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-horizontal: **[Транспорт ZeroMQ](https://docs.rs/ruststream-zeromq/latest/ruststream_zeromq/index.html)** - три паттерна, конечные точки, контракт передачи, запрос и ответ, тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-zeromq)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт документирует только транспорт ZeroMQ. Понятия фреймворка, общие для всех брокеров
(написание подписчиков, публикация, маршрутизация, кодеки, middleware, наблюдаемость, CLI), описаны
в [документации RustStream](https://powersemmi.github.io/ruststream/).
