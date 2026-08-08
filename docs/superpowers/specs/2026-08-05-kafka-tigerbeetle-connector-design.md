# Kafka → TigerBeetle коннектор: дизайн

**Дата:** 2026-08-05
**Статус:** утверждён, готов к планированию реализации
**Модуль:** `github.com/Mi7teR/kafka-tb`

## Цель

Сервис, который принимает финансовые операции из Kafka и применяет их в TigerBeetle, плюс gRPC/REST-API для чтения балансов и выписок. Обёртка скрывает низкоуровневую модель TigerBeetle (u128, битовые флаги, числовые коды, целочисленные суммы) за удобным строковым контрактом.

Два требования определяют дизайн:

1. **Надёжность.** Ни одно сообщение не теряется и не применяется дважды.
2. **Устойчивость к мусору.** Некорректные данные не роняют процесс и не блокируют поток.

## Решения

| Вопрос | Решение |
|---|---|
| Направление | Kafka → TigerBeetle (sink) + read/write API |
| Язык, библиотеки | Go, franz-go, официальный Go-клиент TigerBeetle |
| Идемпотентность | `id` transfer'а задаёт продюсер; `exists` от TB = успех |
| Формат сообщений | Pluggable decoder; JSON в MVP, Protobuf и Avro позже |
| Операции | `create_accounts`, `create_transfers`, two-phase (pending/post/void), атомарные `linked`-цепочки |
| Порядок | Внутри партиции Kafka; продюсер шардит по счёту |
| Ошибки | Poison и reject → DLQ, офсет коммитим; infra → ретрай без коммита |
| Топик результатов | Есть, включается конфигом |
| API | gRPC + grpc-gateway из одного proto; чтение и синхронная запись |
| Топология | Один бинарь, режим флагом `--mode=sink\|api\|all` |
| Гарантия доставки | At-least-once поверх идемпотентности TigerBeetle |

## Архитектура

### Структура пакетов

```
cmd/kafkatb/main.go          --mode=sink|api|all
internal/
  config/       env + yaml, валидация на старте
  model/        внутренние типы: Command{Accounts|Transfers}, Outcome
  codec/        interface Decoder; json/ (MVP), proto/, avro/ — реестр по топику
  tbx/          обёртка TB-клиента: Batcher, submit, маппинг result-кодов
  sink/         franz-go consumer, offset-manager, backpressure, rebalance
  emit/         producer: DLQ-топик + results-топик
  api/          gRPC-сервис + grpc-gateway поверх tbx
  obs/          prometheus, OTel, slog
proto/kafkatb/v1/*.proto     gRPC + REST-аннотации
```

### Batcher — единственная дверь в TigerBeetle

И sink, и API-запись идут через `tbx.Batcher`. Батчер копит `Job` (одно Kafka-сообщение или один API-вызов), формирует запрос до 8189 событий, отправляет, разбирает результаты и будит каждый Job его подмножеством.

Два независимых батчера — `create_accounts` и `create_transfers`: это разные API TigerBeetle, в один запрос не смешиваются.

**Ограничение:** одно сообщение содержит операции только одного типа. Смешение потребовало бы ждать создания счетов перед проводками внутри сообщения. Смешанное сообщение отклоняется как poison.

**Отправка последовательная, один батч in-flight.** TigerBeetle не гарантирует порядок между параллельными запросами, а порядок внутри партиции нам нужен. При батче до 8189 событий узкое место — сам TigerBeetle, не конвейер. Пайплайнинг с привязкой партиций к слотам — возможное будущее улучшение, в MVP не входит.

### Поток обработки

```
franz-go PollFetches
      │  записи по партициям, порядок внутри партиции сохранён
      ▼
decode (codec по топику) ──ошибка──► DLQ(poison) ─► offset ok
      │
      ▼
validate ────────────────ошибка──► DLQ(poison) ─► offset ok
      │
      ▼
Job{records} ──► Batcher.Submit()   (блокирует при заполнении = backpressure)
      │
      ▼
TB create_transfers / create_accounts (≤8189, серийно)
      │
      ├─ infra error ──► backoff-ретрай того же батча, офсет не двигаем
      │
      └─ []results ──► на запись: ok/exists → успех
                                   прочее   → DLQ(reject, код)
                       │
                       ▼
                 emit: results-топик (все исходы) + DLQ где надо
                       │
                       ▼
                 offset-manager: mark(partition, offset)
```

### Офсеты

Автокоммит отключён (`kgo.DisableAutoCommit`). Офсет коммитим только после того, как TigerBeetle подтвердил операцию **и** продюсер DLQ/results получил ack. Иначе при падении теряется факт отказа.

Коммитим непрерывный префикс по каждой партиции: `map[partition]*offsetTracker`, отмечаем завершённые офсеты, двигаем watermark до первой дырки. Периодический коммит раз в секунду плюс принудительный при отзыве партиций.

Гарантия — at-least-once. Exactly-once не нужен: `id` задан продюсером, повторный `create_transfer` возвращает `exists`. Дубликаты в results-топике возможны, потребитель дедуплицирует по `id`.

### Ребаланс и остановка

Стратегия `CooperativeSticky`. На `OnPartitionsRevoked`: перестаём подавать новые Job, дожидаемся in-flight батча, флашим DLQ/results, коммитим, отдаём партиции.

Backpressure отдельного механизма не требует: у батчера ограничена очередь, при заполнении `Submit` блокирует, `PollFetches` не вызывается, franz-go тормозит сам.

Shutdown по SIGTERM: стоп polling → drain → commit → close TB → close producer. По истечении дедлайна из конфига — жёсткий выход без коммита, что безопасно при at-least-once.

## Контракт данных

Обёртка транслирует человекочитаемые поля в модель TigerBeetle.

| Наружу | TigerBeetle | Правило |
|---|---|---|
| `id: "0193f8a1-..."` | `u128` | UUID/ULID как 16 байт, обратно тот же UUID |
| `ledger: "USD"` | `u32` | справочник в конфиге |
| `code: "payment"` | `u16` | справочник в конфиге |
| `amount: "12.34"` | `u128`, минорные единицы | масштаб ledger'а из конфига: `USD` scale 2 → `1234`. Строка, не float |
| `flags: ["linked","pending"]` | битмаска | список имён |
| `user_data_128` | `u128` | UUID-строка |
| `user_data_64`, `user_data_32` | как есть | числа |
| `timeout: "30s"` | `u32` секунд | Go duration-строка |

Пример сообщения:

```json
{
  "operation": "create_transfers",
  "transfers": [
    {
      "id": "0193f8a1-7c2e-7000-8000-000000000001",
      "debit_account_id":  "0193f8a1-0000-7000-8000-000000000010",
      "credit_account_id": "0193f8a1-0000-7000-8000-000000000020",
      "amount": "12.34",
      "ledger": "USD",
      "code": "payment",
      "flags": ["linked"],
      "user_data_128": "0193f8a1-0000-7000-8000-0000000000ff"
    },
    {
      "id": "0193f8a1-7c2e-7000-8000-000000000002",
      "debit_account_id":  "0193f8a1-0000-7000-8000-000000000020",
      "credit_account_id": "0193f8a1-0000-7000-8000-000000000030",
      "amount": "12.34",
      "ledger": "USD",
      "code": "payment",
      "flags": []
    }
  ]
}
```

### Атомарные цепочки

Массив `transfers` внутри сообщения атомарен. Батчер кладёт его целиком в текущий батч либо флашит и начинает новый — цепочка `linked` никогда не разрезается границей батча. Флаг `linked` на последнем элементе снимается автоматически, как требует TigerBeetle. Массив длиннее 8189 элементов — poison.

### Единый proto

`proto/kafkatb/v1/` определяет gRPC-API, Protobuf-кодек сообщений и схему топика результатов. Один источник правды для типов.

### Валидация

До обращения к TigerBeetle отклоняем: неизвестный ledger или code, некорректный UUID, `amount` с числом знаков больше scale, пустой массив, нулевой id, смешанные типы операций в одном сообщении.

## Ошибки и устойчивость

Принцип: коннектор не падает никогда. Единственная причина остановить продвижение по партиции — недоступность TigerBeetle или Kafka.

### Классификация

| Класс | Примеры | Действие | Офсет |
|---|---|---|---|
| Poison | не JSON, неизвестное поле, битый UUID, неизвестный ledger, `amount: "abc"`, массив > 8189, сообщение больше лимита, паника в декодере | DLQ, причина в хедерах | коммит |
| Reject | `exceeds_credits`, `debit_account_not_found`, `exists_with_different_flags`, `linked_event_failed`, `accounts_must_be_different` | DLQ + results, код словом | коммит |
| Infra | TigerBeetle недоступен, таймаут, продюсер не пишет | backoff-ретрай без ограничения попыток | не коммитим |
| Успех | `ok`, `exists` | results | коммит |

### Защита от некорректных данных

- `recover()` вокруг обработки каждого сообщения. Паника в декодере или маппере превращается в poison-DLQ со стектрейсом в хедере, обработка продолжается.
- Лимиты в конфиге: размер сообщения, число элементов в массиве, глубина JSON. Проверяются до парсинга, где это возможно.
- Строгий декодинг (`DisallowUnknownFields`). Опечатка в имени поля — poison, а не молча применённый нулевой amount.
- Ни одна ошибка данных не блокирует партицию. Head-of-line blocking возможен только на классе infra, и там ожидание — правильное поведение.
- TigerBeetle — последний рубеж: даже пропущенную валидатором проблему он отвергнет кодом ошибки, что даёт reject → DLQ.

### Формат DLQ

Ключ исходный. Значение — исходные байты без изменений, чтобы реплей был возможен. Метаданные в хедерах:

```
x-kafkatb-reason      poison | reject
x-kafkatb-error       exceeds_credits
x-kafkatb-detail      transfers[3]: amount has 3 decimals, ledger USD scale=2
x-kafkatb-src-topic
x-kafkatb-src-partition
x-kafkatb-src-offset
x-kafkatb-src-timestamp
x-kafkatb-attempt-ts
```

Реплей из DLQ безопасен: `id` тот же, TigerBeetle вернёт `exists`, двойного списания не будет.

Если продюсер DLQ сам не может писать — это класс infra: ретрай, офсет не двигаем. Молчаливый дроп запрещён.

### Надёжность инфраструктуры

- Один TB-клиент на процесс, реконнект внутри клиента, наш ретрай сверху с jitter.
- Идемпотентный продюсер для DLQ и results, `acks=all`.
- `/healthz` — процесс жив. `/readyz` — TigerBeetle отвечает и consumer в группе.
- Метрики: `records_total{result}`, `dlq_total{reason,error}`, `tb_batch_size`, `tb_latency_seconds`, `offset_commit_lag{topic,partition}`. Рост `dlq_total{reason="poison"}` — сигнал о сломанном продюсере. `offset_commit_lag` — разрыв между самым свежим прочитанным офсетом и закоммиченным ватермарком; не путать с брокерским consumer lag (расстояние до конца партиции на брокере) — тот потребовал бы отдельного запроса к брокеру за log-end-offset и здесь не реализован.

## API

Один `proto/kafkatb/v1/kafkatb.proto` даёт gRPC на порту 9090 и REST/JSON через grpc-gateway на 8080. Типы те же, что в Kafka-сообщениях.

```protobuf
service Ledger {
  // чтение
  rpc GetAccounts(GetAccountsRequest) returns (GetAccountsResponse);
      // GET /v1/accounts?id=...&id=...
  rpc GetTransfers(GetTransfersRequest) returns (GetTransfersResponse);
      // GET /v1/transfers?id=...
  rpc ListAccountTransfers(ListAccountTransfersRequest) returns (ListAccountTransfersResponse);
      // GET /v1/accounts/{account_id}/transfers
  rpc ListAccountBalances(ListAccountBalancesRequest) returns (ListAccountBalancesResponse);
      // GET /v1/accounts/{account_id}/balances
  rpc QueryTransfers(QueryTransfersRequest) returns (QueryTransfersResponse);
      // GET /v1/transfers:query?user_data_128=...&code=payment
  rpc QueryAccounts(QueryAccountsRequest) returns (QueryAccountsResponse);

  // запись
  rpc CreateAccounts(CreateAccountsRequest) returns (CreateAccountsResponse);
      // POST /v1/accounts
  rpc CreateTransfers(CreateTransfersRequest) returns (CreateTransfersResponse);
      // POST /v1/transfers
}
```

`ListAccountBalances` требует, чтобы счёт был создан с флагом `history`.

### Ответ на запись

Бизнес-отказ не является HTTP-ошибкой. Ответ `200` с исходом по каждому элементу:

```json
{ "results": [
  { "id": "0193f8a1-7c2e-7000-8000-000000000001", "status": "ok" },
  { "id": "0193f8a1-7c2e-7000-8000-000000000002",
    "status": "rejected",
    "error": "exceeds_credits",
    "detail": "debit account ...0010 balance 5.00 < 12.34" }
]}
```

`4xx` — только невалидный запрос. `503` — TigerBeetle недоступен.

### Ответ на чтение баланса

```json
{ "id": "0193f8a1-0000-7000-8000-000000000010",
  "ledger": "USD", "code": "customer",
  "debits_posted": "1250.00", "credits_posted": "1400.00",
  "debits_pending": "0.00", "credits_pending": "12.34",
  "balance": "150.00" }
```

Поле `balance` вычисляется по флагам счёта: при `credits_must_not_exceed_debits` это `debits - credits`, иначе `credits - debits`. Клиенту не нужно знать, дебетовый счёт или кредитовый.

Пагинация — курсор по `timestamp`, в ответе `next_cursor`. Лимит с потолком из конфига.

Аутентификация в MVP не реализуется: сервис работает за mesh или ingress. В конфиге предусмотрен хук на interceptor.

## Конфигурация

YAML с переопределением через переменные окружения `KAFKATB_*`. Конфиг валидируется на старте — сервис падает сразу, а не на первом сообщении.

```yaml
mode: all                      # sink | api | all
tigerbeetle:
  cluster_id: 0
  addresses: ["3000", "3001", "3002"]
batcher:
  max_batch_size: 8189
  linger: 5ms
  max_queue: 50000
kafka:
  brokers: ["localhost:9092"]
  group: kafkatb
  topics:
    - { name: ledger.transfers, codec: json }
  dlq_topic: ledger.transfers.dlq
  results_topic: ledger.results     # пусто — выключено
limits:
  max_message_bytes: 1048576
  max_events_per_message: 8189
  max_json_depth: 32
api:
  grpc_addr: ":9090"
  http_addr: ":8080"
  max_page_size: 1000
ledgers:
  USD: { id: 1, scale: 2 }
  EUR: { id: 2, scale: 2 }
codes:
  payment: 1
  refund: 2
retry:
  initial: 100ms
  max: 30s
  jitter: true
shutdown_timeout: 30s
```

## Тестирование

Разработка идёт от тестов: сначала тест на инвариант, затем код. Инварианты здесь важнее фич.

### Unit

- **codec** — таблица «некорректный вход → ожидаемая poison-ошибка». Фаззинг JSON-декодера (`go test -fuzz`) с целью: ни одной паники наружу.
- **amount** — конверсия строка ↔ u128 по scale, границы u128, отрицательные значения, избыточные знаки после запятой.
- **batcher** — property-тесты на инварианты: цепочка `linked` не разрезается границей батча, порядок Job сохраняется, размер батча не превышает 8189.
- **offset-manager** — коммитится только непрерывный префикс, дырка блокирует продвижение watermark.

### Integration (testcontainers: Redpanda + TigerBeetle single-replica)

- Happy path: N сообщений применены, балансы сошлись.
- Идемпотентность: топик проигран дважды, балансы те же, второй прогон целиком в `exists`.
- Мусор вперемешку с валидными сообщениями: некорректные в DLQ, валидные применены, лаг не растёт.
- Убийство сервиса посреди батча, рестарт: нет потерь и нет двойного списания.
- Reject: списание сверх баланса даёт DLQ с `exceeds_credits`, поток продолжается.
- Реплей из DLQ: `exists`, балансы не изменились.
- API: создание через REST, затем чтение баланса, значения сходятся.

### Бенчмарки

Две группы: микробенчмарки горячего пути (`go test -bench`) и нагрузочный прогон всего конвейера.

**Микро** — считаем ns/op и allocs/op, гоняем в CI, ловим регрессии через `benchstat`:

- `BenchmarkDecodeJSON` — декодинг сообщения на 1, 10, 100 transfer'ов.
- `BenchmarkAmountParse` / `BenchmarkAmountFormat` — конверсия строка ↔ u128.
- `BenchmarkUUIDToU128` — маппинг идентификаторов.
- `BenchmarkBatcherAssemble` — сборка батча из очереди Job, включая проверку целостности цепочек.
- `BenchmarkMapResults` — разбор ответа TigerBeetle на 8189 элементов.

Цель по аллокациям: путь «байты из Kafka → структура TigerBeetle» не должен аллоцировать на элемент больше одного раза. Переиспользуем буферы через `sync.Pool`, декодим в заранее выделенный слайс.

**Нагрузочный харнесс** (`cmd/loadgen`, отдельный бинарь, не в проде):

- Заливает в топик N миллионов синтетических transfer'ов с заданным шардированием по счетам.
- Гоняет коннектор против TigerBeetle (single-replica локально, кластер из трёх — в стейджинге).
- Снимает: transfers/сек, p50/p95/p99 end-to-end (timestamp продюсера → коммит офсета), гистограмму размера батча, latency TigerBeetle отдельно от общей, consumer lag во времени.

**Сценарии:**

| Сценарий | Что проверяем |
|---|---|
| Только записи, счета не пересекаются | Потолок пропускной способности |
| Горячий счёт (все проводки в один) | Деградация от конкуренции внутри TigerBeetle |
| `linger` 1мс против 50мс | Кривая throughput/latency, подбор дефолта |
| Смесь с 5% мусора | Накладные расходы DLQ-пути |
| Синхронный API-write под фоновым потоком из Kafka | Влияние общего батчера на p99 API |
| Цепочки `linked` по 10 элементов | Стоимость сохранения атомарности при упаковке |

Цифры фиксируем в `docs/benchmarks/` с версией, конфигом и железом. Смысл — не абсолютные значения, а сравнимость между коммитами.

## Вне скоупа MVP

- Аутентификация и авторизация API.
- Пайплайнинг батчей с привязкой партиций.
- Кодеки Protobuf и Avro (интерфейс есть, реализация позже).
- CDC в обратную сторону (TigerBeetle → Kafka).
- Kafka Connect как рантайм.
