# GoZon microservices (Orders + Payments)

Учебная реализация интернет-магазина с двумя микросервисами и API Gateway. Решение демонстрирует
идемпотентные платежи, transactional outbox/inbox и гарантию effectively exactly once при списании
денег за заказ.

## Сервисы
- **API Gateway** — простой reverse-proxy, пробрасывает запросы в Orders и Payments.
- **Orders Service** — создание заказов, список и статус, transactional outbox для задач оплаты, inbox для событий оплаты.
- **Payments Service** — создание/пополнение счёта, просмотр баланса, inbox + outbox для сообщений оплаты, списание атомарным апдейтом.
- **RabbitMQ** — брокер сообщений с at-least-once доставкой.
- **PostgreSQL** — отдельные БД для orders и payments.

## Как запустить
```bash
docker compose up --build
```
Сервисы поднимаются на портах:
- Gateway: `http://localhost:8080`
- Orders Service (прямой доступ): `http://localhost:8081`
- Payments Service (прямой доступ): `http://localhost:8082`
- RabbitMQ UI: `http://localhost:15672` (guest/guest)

Каждый запрос требует заголовок `X-User-Id` (имитируем аутентификацию в инфраструктуре компании).

## Основные эндпойнты (через Gateway)
### Payments
- `POST /payments/accounts` — создать счёт (не более одного на пользователя). Тело: `{ "initialBalance": 0 }` (необязательно).
- `POST /payments/accounts/deposit` — пополнить счёт. Тело: `{ "amount": 100 }`.
- `GET /payments/accounts/balance` — получить баланс.

### Orders
- `POST /orders/orders` — создать заказ и запустить оплату. Тело: `{ "amount": 150, "description": "Набор" }`.
- `GET /orders/orders` — список заказов пользователя.
- `GET /orders/orders/{id}` — статус заказа (`NEW`, `FINISHED`, `CANCELLED`).

## Сообщения и идемпотентность
- **Orders ➜ Payments:** `order.payment.requested` (Transactional Outbox в Orders).
- **Payments Inbox:** таблица `payment_inbox` хранит полученные сообщения и предотвращает двойную обработку.
- **Payments ➜ Orders:** `payment.status.changed` (Payments Outbox).
- **Orders Inbox:** таблица `payment_event_inbox` предотвращает повторные изменения статуса.
- Списание баланса выполняется атомарным `UPDATE ... WHERE balance >= amount`, что исключает гонки.

## Swagger и Postman
Swagger включён во всех сервисах (`/swagger`).
Готовая Postman-коллекция: `docs/GoZon.postman_collection.json`.

## Переменные окружения
| Сервис | Переменная | Значение по умолчанию |
| --- | --- | --- |
| Orders | `ORDERS_CONNECTION_STRING` | `Host=orders-db;Port=5432;Database=ordersdb;Username=postgres;Password=postgres;` |
| Orders | `RABBITMQ_HOST` | `rabbitmq` |
| Payments | `PAYMENTS_CONNECTION_STRING` | `Host=payments-db;Port=5432;Database=paymentsdb;Username=postgres;Password=postgres;` |
| Payments | `RABBITMQ_HOST` | `rabbitmq` |
| Gateway | `ORDERS_BASE_URL` | `http://orders-service:8080` |
| Gateway | `PAYMENTS_BASE_URL` | `http://payments-service:8080` |

Запуск через compose автоматически выставляет все значения.
