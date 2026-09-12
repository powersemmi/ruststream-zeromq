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

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-horizontal: **[Руководство по ZeroMQ](zeromq.md)** - три паттерна, конечные точки, контракт передачи, запрос и ответ, тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-zeromq)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт документирует только транспорт ZeroMQ. Понятия фреймворка, общие для всех брокеров
(написание подписчиков, публикация, маршрутизация, кодеки, middleware, наблюдаемость, CLI), описаны
в [документации RustStream](https://powersemmi.github.io/ruststream/).
