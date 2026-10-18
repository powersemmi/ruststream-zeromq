---
title: Запрос и ответ
sidebar_position: 4
description: "Ответы на запросы на DEALER/ROUTER обычным обработчиком в reply-форме и запрос к другому сервису из обработчика с таймаутом и повтором позже."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Запрос и ответ

Это руководство отвечает на запросы на DEALER/ROUTER и спрашивает другой сервис изнутри обработчика.

В примере два сервиса.
Сервис оценки отвечает, одобрена ли сумма заказа для клиента.
Сервис оформления заказов спрашивает его о каждом принятом заказе.

## Отвечающая сторона

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

[`ZmqRpc`](../components/broker/zmq-rpc.md) - паттерн DEALER/ROUTER.
Отвечающая сторона устойчива, поэтому она привязывается к адресу, а каждая запрашивающая сторона подключается к ней.
`score` - обычный обработчик в reply-форме: он возвращает ответ или отбрасывает запрос без суммы.

Сокет ROUTER знает, какой узел отправил каждый запрос.
Он записывает этот узел в заголовок запроса `reply-to`, и ответ должен уйти туда.
Поэтому `Score` не объявляет своего адреса назначения, а `reply("reply")` называет только запасной адрес.
Преобразование ниже заменяет его для каждого запроса.

Отвечающая сторона называет, куда уходят её [копии для повтора](../components/settlement/out-retry.md): `.out_retry(Publish).to("score.retry")`.
У копии запроса нет своего адреса, поэтому регистрацию без этого шага старт отвергает.
[Пакеты](../components/subscriptions/batches.md) отвечающая сторона тоже не принимает: `.batch(..)` на ней не компилируется, потому что каждый ответ уходит своему узлу.

## Преобразование

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#transform"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#transform"
```

</TabItem>
</Tabs>

`ReplyToRequester` - [преобразование публикации](../components/request-reply/responders.md) на ответе.
Оно читает заголовок `reply-to` запроса и отправляет ответ туда.
Ещё оно копирует `correlation-id` запроса: по нему запрашивающая сторона сопоставляет ответ с запросом.
Оно объявляет `Destination = Names`, потому что само выбирает адрес назначения.
Так преобразование может поступать, только если тип ответа не объявляет адреса, поэтому тип ответа с именем и это преобразование вместе не компилируются.

Ответ узлу, который уже отключился, отвергается с ошибкой, в которой назван его адрес.
Так отвечающая сторона узнаёт, что ответ никуда не дошёл. На широковещательной конечной точке сообщение без совпавшего фильтра отбрасывается молча.

## Запрашивающая сторона

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply_client.rs#requester"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply_client.rs#requester"
```

</TabItem>
</Tabs>

`check` принимает параметр `Out<impl RequestReply>`, а точка монтирования связывает его с политикой `ZmqRpc` через `.out(DefaultSlot, ask)`.
Брокер `scoring` подключается к сервису оценки и подписки не имеет: он только спрашивает.
[`request`](../components/request-reply/index.md) отправляет запрос через собственный сокет DEALER и возвращает ответ с тем же `correlation-id`.
`check` сам кодирует запрос и декодирует ответ в JSON, который читает и пишет сервис оценки.

## Таймауты

Если ответ не пришёл за `ASK_TIMEOUT`, `request` возвращает ошибку [`ZmqError::RequestTimeout`](../components/broker/errors.md).
Тогда `check` возвращает [`retry_after(ASK_AGAIN)`](../components/settlement/retries.md), и заказ возвращается через десять секунд.
`max_attempts` ограничивает число попыток тремя.
Это паттерн Lazy Pirate из [руководства ZeroMQ](https://zguide.zeromq.org/docs/chapter4/#Client-Side-Reliability-Lazy-Pirate-Pattern), только попытки считает фреймворк.
Запрос с истёкшим таймаутом мог всё же дойти до отвечающей стороны, поэтому повторная отправка запроса должна быть безопасной.

Ни `request`, ни публикацию ответа нельзя безопасно отменить.
Обработчик прекращает ожидание по таймауту запроса, а не через ветку `select!` ([отмена](../components/publishing/cancellation.md)).

## Тестирование

Тесты сервиса оценки отправляют запросы так, как это сделал бы DEALER запрашивающей стороны, и проверяют, как `score` завершил их доставку.
Тест оформления заказа работает во внутрипроцессном режиме без сервиса оценки, поэтому его запрос истекает по таймауту.
Он проверяет, что `check` попросил повтора и что копия вернулась через `ASK_AGAIN` со счётчиком повторов, равным единице.
Часы остановлены, поэтому таймаут и задержка проходят мгновенно.
Обвязку разбирает руководство [Тестирование сервиса на ZeroMQ](testing.md).
