# Каталог микросервисов

Каталог — сводка по девяти сервисам. Для каждого сервиса есть отдельный детальный документ (каталог `docs/services/`). В нём:

- диаграмма C2 сервиса — его контейнеры и соседи;
- диаграмма C3 для каждого контейнера: и API, и каждого worker;
- полное описание API — REST, события Kafka и MQTT;
- схема хранения с индексами, партиционированием и шардированием, масштабирование.

Общие правила API и событий: [api-conventions.md](api-conventions.md). Хранение данных и выбор PostgreSQL: [data-storage.md](data-storage.md) и [ADR-002](adr/ADR-002-storage.md).

Для всех сервисов:

- Свою БД изменяет только сервис-владелец. Идентификаторы чужих агрегатов не создают межсервисных SQL-связей.
- API версионируются (`/api/v1`, `/admin/v1`, `/internal/v1`). Команды принимают `Idempotency-Key`. Внутренние вызовы аутентифицируются через mTLS и allowlist вызывающих контейнеров.
- **Workers — отдельные контейнеры** (deployment) со своим масштабированием и своей C3-диаграммой. Они не встраиваются в процесс API.
- Публикацию outbox в Kafka выполняет платформенный **Outbox CDC** (Kafka Connect + Debezium). Он читает WAL сервисных БД, поэтому у сервисов нет собственных relay-процессов.
- Все кластеры PostgreSQL — версии 17+: logical-слоты Debezium создаются с `failover = true` и переживают переключение primary ([data-storage.md §2.2](data-storage.md#22-версия-и-расширения)). Окна PITR ([data-storage.md §8.5](data-storage.md#85-бэкапы-и-pitr)): Billing — 35 сут; Trip, Fleet, Pricing, Photo, Geofence, User — 14 сут; Telemetry — 7 сут. Discovery Index не восстанавливается из бэкапа, а перестраивается.
- Данные старше горячего окна выгружаются в Parquet в платформенное архивное хранилище **Archive Store** (S3-совместимое в РФ, SSE, versioning и Object Lock на весь срок хранения) — [data-storage.md §6.4](data-storage.md#64-архивирование-detach--parquet--drop).
- Технологии — проектный выбор. Достижение требуемой нагрузки подтверждается измерениями.

Зависимости ниже разделены на **синхронные** и **событийные**: отсутствие прямого RPC не означает отсутствия зависимости от поступления данных.

## Сводная таблица

| # | Сервис | Bounded context | Контейнеры | Хранилище | Документ |
| ---: | --- | --- | --- | --- | --- |
| 1 | User Service | User Management | User API, User Worker | User DB (PostgreSQL) | [user-service.md](services/user-service.md) |
| 2 | Fleet Service | Fleet & IoT | Fleet API, Fleet Command Worker, Fleet Event Worker | Fleet DB (PostgreSQL) | [fleet-service.md](services/fleet-service.md) |
| 3 | Telemetry Service | Fleet & IoT | Telemetry Ingestor, Telemetry API | Telemetry DB (PostgreSQL + TimescaleDB) | [telemetry-service.md](services/telemetry-service.md) |
| 4 | Discovery Service | Discovery | Discovery API, Discovery Projector | Discovery Index (Redis Cluster) | [discovery-service.md](services/discovery-service.md) |
| 5 | Trip Service | Rental | Trip API, Trip Saga Worker | Trip DB (PostgreSQL + read replica) | [trip-service.md](services/trip-service.md) |
| 6 | Pricing Service | Pricing | Pricing API | Pricing DB (PostgreSQL) | [pricing-service.md](services/pricing-service.md) |
| 7 | Billing Service | Billing | Billing API, Billing Settlement Worker, Billing Reconciliation Worker | Billing DB (PostgreSQL) | [billing-service.md](services/billing-service.md) |
| 8 | Geofence Service | Geofencing | Geofence API | Geofence DB (PostgreSQL + PostGIS) | [geofence-service.md](services/geofence-service.md) |
| 9 | Photo Service | Photo Evidence | Photo API, Photo Processing Worker | Photo DB (PostgreSQL) + Photo Object Storage (S3) | [photo-service.md](services/photo-service.md) |

Итого 18 прикладных контейнеров. У Pricing и Geofence worker-ов нет, потому что у них нет фоновой работы: события публикует Outbox CDC, а кеш правил Geofence обновляется внутри API.

## 1. User Service

**Bounded context:** User Management. **Владелец:** команда пользователей. **Детально:** [user-service.md](services/user-service.md).

**Responsibility:** регистрация и вход по телефону с OTP, профиль, согласия, статус аккаунта, право на начало аренды, удаление аккаунта (152-ФЗ). Финансовые ограничения сервис получает как факты от Billing. Из-за долга завершение текущей поездки не запрещается.

**Containers:**

- **User API** — OTP, токены, профиль, `rental-eligibility`, JWKS.
- **User Worker** — пересчёт ограничений по событиям Billing, обработка заявок на удаление, очистка сессий.

**Owned Data:** `User`, `PhoneIdentity`, `OtpChallenge`, `RefreshSession`, `UserAccessStatus`, `AccessBlock`, `Consent`. Долг пользователя хранится только как локальная проекция и не является финансовым источником истины.

**Storage:** User DB (PostgreSQL). Объём около 40 ГБ (оценка; из них ≈ 10 ГБ — учётные записи и профили, остальное — сессии, согласия, inbox и аудит входов), поэтому шардирование не требуется.

**API (кратко):**

- `POST /api/v1/auth/otp`, `POST /api/v1/auth/token`, `POST /api/v1/auth/logout`.
- `GET/PATCH /api/v1/users/me`, `POST /api/v1/users/me/consents`, `DELETE /api/v1/users/me`.
- `GET /internal/v1/users/{userId}/rental-eligibility` — право на старт.
- `GET /.well-known/jwks.json` — ключи проверки JWT.
- `/admin/v1/users/...` — просмотр и блокировки.

**Dependencies:**

- Синхронно — SMS-шлюз.
- Событийно — Billing (`PaymentStatusChanged`, `FineIssued`, `FineStatusChanged`).

**Communication:** HTTPS/REST. Публикует `UserAccessChanged` (потребители — Trip Saga Worker, Photo Processing Worker, Billing Settlement Worker). Клиентские JWT проверяются Gateway и сервисами локально по JWKS; ключи подписи JWT хранятся в Vault/KMS. Бизнес-право старта проверяет Trip. Операторов User Service не аутентифицирует: их JWT выпускает корпоративный OIDC IdP (см. [инфраструктуру](#инфраструктурные-компоненты)).

## 2. Fleet Service

**Bounded context:** Fleet & IoT. **Владелец:** команда IoT/парка. **Детально:** [fleet-service.md](services/fleet-service.md).

**Responsibility:** учёт самокатов, техническая готовность, исключительное выделение самоката поездке (lease с fencing token), команды замку и сверка с устройством, отчёты о повреждениях. Состояние поездки и её цена сервису не принадлежат.

**Containers:**

- **Fleet API** — lease, приём команд, обслуживание, отчёты о повреждениях.
- **Fleet Command Worker** — отправка команд в MQTT, приём результатов и состояния, тайм-ауты, `QUERY_STATE`, истечение подготовительных lease.
- **Fleet Event Worker** — проекция телеметрии: последнее подписанное измерение, заряд, online. Также принимает статусы фото к отчётам о повреждениях.

**Owned Data:** `Scooter`, `ScooterLease`, `DeviceCommand`, `DeviceState`, `DamageReport`, outbox/inbox. Последнее измерение — проекция данных Telemetry. Инвариант «один lease на самокат» обеспечивает непартиционированная `scooter_active_lease`.

**Storage:** Fleet DB (PostgreSQL), синхронная реплика. Стабильное состояние lease и команд не хранится только в Redis.

**API (кратко):**

- Внутренние, `/internal/v1`:
  - `GET /scooters/{scooterId}` — для Trip Saga Worker и Telemetry API (город самоката для проверки городов оператора);
  - `POST /scooters/{scooterId}/leases`;
  - `POST /leases/{leaseId}/commands`;
  - `GET /commands/{commandId}`;
  - `POST /leases/{leaseId}/release`;
  - `GET /leases/{leaseId}` — состояние lease для сверки в Trip;
  - `GET /scooters/snapshot`.
- Публичный: `POST /api/v1/scooters/{scooterId}/damage-reports`.
- `/admin/v1/scooters/...`, `/admin/v1/damage-reports/...`, `POST /admin/v1/leases/{leaseId}/force-release` (только `ADMIN`, последнее средство для `QUARANTINED` lease).
- MQTT: `devices/{scooterId}/commands`, `command-results`, `state`. Требования к прошивке (fencing token во флеш-памяти, `expiresAt`, дедуп по `commandId`, `connSeq`) — в [fleet-service.md §3.5](services/fleet-service.md#35-mqtt).

**Dependencies:**

- Синхронно — MQTT и устройство; Telemetry API, если сохранённое измерение устарело; Photo API (`GET /internal/v1/photos/{photoId}`) для фото отчёта о повреждении, статус которых ещё не пришёл событием.
- Событийно — `telemetry.location.v1`, `photo.status.v1`.

Сервис не вызывает Trip, чтобы решить, свободен ли самокат.

**Communication:** REST для Trip и операторов, MQTT с устройством. Публикует в Kafka `ScooterAvailabilityChanged` и `DeviceCommandUpdated`. ACK MQTT подтверждает доставку сообщения, а бизнес-подтверждение устройства — фактическое исполнение; это разные факты.

## 3. Telemetry Service

**Bounded context:** Fleet & IoT. **Владелец:** команда IoT/парка. **Детально:** [telemetry-service.md](services/telemetry-service.md).

**Responsibility:** приём, нормализация и дедупликация GPS, заряда и heartbeat; отбраковка некорректных измерений; **подпись измерений** (Ed25519) для неотрекаемых геопроверок. Сервис не назначает поездку и не разрешает разблокировку.

**Containers:**

- **Telemetry Ingestor** — MQTT shared subscription, пакетная запись, подпись, публикация `LocationObserved`.
- **Telemetry API** — последнее измерение, snapshot, JWKS ключей подписи, история по ролям.

**Owned Data:** `LocationMeasurement`, `LastLocation`, `DeviceHeartbeat`. Ключ подписи Ed25519 хранится и ротируется в Vault Transit; поды Ingestor получают его версии и подписывают в памяти (20 000 подписей/с через API Vault не пропустить). Сервис хранит и публикует открытые ключи (JWKS).

**Storage:** Telemetry DB (PostgreSQL + TimescaleDB). Hypertable с чанками по 1 ч, сжатие, сырые данные хранятся 30 дней, минутные агрегаты — 1 год.

**API (кратко):**

- Внутренние: `GET /internal/v1/scooters/{scooterId}/location`, `GET /internal/v1/locations/snapshot`, `GET /internal/v1/signing-keys`.
- Операторская история `/admin/v1/telemetry/...`.
- Основной вход — MQTT `devices/{scooterId}/telemetry`.

**Dependencies:** MQTT-брокер, собственная БД, Kafka и Vault Transit (подпись). Для приёма измерения RPC в Fleet не требуется. Telemetry API вызывает Fleet `GET /internal/v1/scooters/{scooterId}` только для операторской истории — чтобы проверить, что самокат в городе оператора.

**Communication:** MQTT на входе. `LocationObserved` публикуется в Kafka после коммита в БД (без outbox) для Discovery и Fleet. Идентификатор измерения детерминирован — UUIDv5 от `scooterId|bootId|seq`, поэтому повторы безопасны. Каноническая строка подписи (`TLM1|…`) определена в [telemetry-service.md](services/telemetry-service.md).

**Режимы деградации** ([telemetry-service.md §2.1.3](services/telemetry-service.md#213-режимы-деградации)): `DB_ONLY` — Kafka недоступна, PUBACK после коммита в БД, события не публикуются; `KAFKA_ONLY` — БД недоступна дольше 10 с, все валидные измерения публикуются, PUBACK после подтверждения Kafka, после восстановления БД окно отказа дозаписывается из топика (ретенция 24 ч); `STALLED` — недоступны оба, приём приостанавливается.

## 4. Discovery Service

**Bounded context:** Discovery. **Владелец:** команда поиска/карты. **Детально:** [discovery-service.md](services/discovery-service.md).

**Responsibility:** поиск ближайших самокатов, фильтрация доступности, отображение зон и ориентировочной цены. Поиск не резервирует ресурс.

**Containers:**

- **Discovery API** — поиск, карточка, карта зон.
- **Discovery Projector** — построение геоиндекса по событиям и перестроение из snapshot.

**Owned Data:** производная проекция: GEO-набор доступных самокатов, карточки, кеш тарифов и зон, версии обработанных событий. Владельцы `Scooter`, `Location`, `Tariff`, `Geofence` остаются в исходных сервисах.

**Storage:** Discovery Index (Redis Cluster, hash tag `{cityId}`). Индекс восстанавливается из snapshot владельцев и последующих событий.

**API (кратко):**

- `GET /api/v1/scooters/nearby`, `GET /api/v1/scooters/{scooterId}/card`, `GET /api/v1/map/geofences`.
- `/admin/v1/discovery/rebuilds`.

**Dependencies:**

- Синхронный fan-out в поисковом запросе отсутствует.
- Событийно — Fleet, Telemetry, Pricing, Geofence.
- Для перестроения — snapshot API Fleet и Telemetry, текущие версии тарифов Pricing (`GET /internal/v1/tariff-versions/current`; также досинхронизация тарифов после compaction топика) и версии зон Geofence (`GET /internal/v1/geofence-versions/current`, `/{versionId}`).

## 5. Trip Service

**Bounded context:** Rental. **Владелец:** команда аренды. **Детально:** [trip-service.md](services/trip-service.md).

**Responsibility:** жизненный цикл поездки; durable saga старта и завершения — проверки, котировка, lease, авторизация, разблокировка и блокировка; восстановление незавершённых операций. Сервис проверяет, что фото парковки принадлежит поездке, и ведёт ручной разбор спорных завершений.

**Containers:**

- **Trip API** — принимает команды. В одной транзакции записывает намерение и шаг saga, затем отвечает `202`. Синхронно другие сервисы не вызывает.
- **Trip Saga Worker** — исполняет шаги, вызывает сервисы, выполняет компенсации, потребляет события, публикует `TripEnded`.

**Owned Data:** `Trip`, `TripTransition`, `TripOperation`, `SagaStep`, `UserActiveTrip`, `TripReview`, `IdempotencyRecord`, outbox/inbox. Snapshot тарифа, результаты геопроверок, статусы фото и платёжные статусы — копии фактов, а не право изменять исходные сущности.

**Storage:** Trip DB (PostgreSQL с синхронной репликой и read replica для истории).

- Инвариант «одна незавершённая поездка» — непартиционированная `user_active_trip`.
- Поездки партиционированы помесячно; в горячей БД — 13 месяцев, затем партиции выгружаются в Parquet в Archive Store и хранятся 5 лет под Object Lock (с versioning).
- Шардирование готово по `userId`.

**API (кратко):**

- Старт: `POST /api/v1/trips` → `202`.
- Чтение: `GET /api/v1/trips/{tripId}`, `GET /api/v1/trips`.
- Завершение: `POST /api/v1/trips/{tripId}/finish` с `parkingPhotoId` → `202`.
- `GET /api/v1/trips/{tripId}/operations/{operationId}` — статус операции. START: `ACCEPTED`, `CHECKING`, `AUTHORIZING`, `UNLOCKING`, `COMPLETED`, `CANCELLED`, `RECONCILING`; FINISH: `ACCEPTED`, `CHECKING_PARKING`, `LOCKING`, `COMPLETED`, `REJECTED`, `RECONCILING`.
- Внутренний (единственный внутренний эндпоинт Trip): `GET /internal/v1/trips/{tripId}` → `{tripId, userId, status, authorizationId, startedAt, endedAt}`; allowlist `billing-reconciliation-worker` — сверка осиротевших hold по фактическому статусу поездки ([trip-service.md §3.2.7](services/trip-service.md#327-get-internalv1tripstripid--статус-поездки-для-сверки-hold-добавлен)).
- `/admin/v1/trips/...` — принудительное завершение, решения сверки, повтор шага saga; `/admin/v1/trip-reviews/...` — очередь ручного разбора и решения оператора.

**Dependencies:**

- Синхронно (Saga Worker) — User, Pricing (котировка, `GET /internal/v1/signing-keys`), Fleet (включая `GET /internal/v1/leases/{leaseId}` при сверке), Geofence (`geo-checks/start`, `geo-checks/finish`, `GET /internal/v1/geo-checks/{checkId}` при разборе `trip_review`), Billing (`payment-authorizations`: создание, опрос исхода, отмена), Photo (`GET /internal/v1/photos/{photoId}` при сверке фото парковки, если событие не пришло).
- Событийно — `fleet.device-command.v1`, `user.access.v1`, `billing.payment-status.v1`, `billing.fine.v1`, `photo.status.v1`.

Завершение не требует ответа Billing, Pricing или Photo. `TripEnded.finishCheck` содержит `checkId`, `decision`, `reasonCode`, `policyVersion` (целое) и `violationEvidenceId`; при принудительном завершении без геопроверки `decision = NOT_CHECKED`, остальные поля — `null`. Строка `user_active_trip` удаляется при терминальном переходе; исключение — `RECONCILING` после завершения: через 10 мин поездка передаётся оператору и строка удаляется досрочно, а самокат остаётся под lease до решения оператора. Штраф после ручного разбора оператор назначает в Billing (`POST /admin/v1/fines`), Trip Billing для этого не вызывает.

## 6. Pricing Service

**Bounded context:** Pricing. **Владелец:** команда тарификации. **Детально:** [pricing-service.md](services/pricing-service.md).

**Responsibility:** версии тарифов, котировки с подписанным snapshot, детерминированный расчёт цены по зафиксированным условиям. Сервис не списывает деньги и не принимает решение о виновности пользователя.

**Containers:** **Pricing API** (worker не нужен).

**Owned Data:** `Tariff`, `TariffVersion`, `PriceRule`, `Quote` с TTL, `PriceCalculation`, версия алгоритма. Опубликованные версии не изменяются.

**Storage:** Pricing DB (PostgreSQL). Котировки — суточные партиции, хранение 7 дней. Журнал расчётов (`price_calculation`) — помесячные партиции, 13 месяцев в горячей БД, затем Parquet в Archive Store на 5 лет с конца года расчёта. Кеш опубликованных версий хранится в памяти.

**API (кратко):**

- Публичный: `GET /api/v1/tariffs`.
- Внутренние: `POST /internal/v1/quotes` (Trip Saga Worker), `POST /internal/v1/price-calculations` (только Billing Settlement Worker), `GET /internal/v1/tariff-versions/current` (Discovery Projector), `GET /internal/v1/signing-keys` — открытые ключи проверки подписи котировок (Trip Saga Worker, Billing Settlement Worker).
- `/admin/v1/tariffs/...` — версии и публикация; `/admin/v1/price-calculations` — просмотр расчётов.

**Dependencies:** синхронных бизнес-зависимостей нет; закрытый ключ подписи котировок хранится в Vault/KMS. Публикует `TariffVersionPublished`. `cityId` — UUID; человекочитаемый код города может быть только отдельным справочным полем, не ключом.

## 7. Billing Service

**Bounded context:** Billing. **Владелец:** команда расчётов. **Детально:** [billing-service.md](services/billing-service.md).

**Responsibility:** авторизация, списание, возврат, финансовый учёт, долг, штрафы и обжалования. Самокат не остаётся «в поездке» из-за неуспешного списания.

**Containers:**

- **Billing API** — синхронная авторизация у PSP, способы оплаты, платежи, штрафы и обжалования, приём webhook.
- **Billing Settlement Worker** — по `TripEnded` рассчитывает сумму через Pricing и выполняет списание; ведёт долг, повторы, возвраты и назначение штрафов (`FinePolicy` выбирается по `(cityId, finishCheck.reasonCode)` на `endedAt`; для `NOT_CHECKED` и принудительного завершения автоматического штрафа нет). По `UserAccessChanged(DELETED)` деактивирует способы оплаты.
- **Billing Reconciliation Worker** — обработка webhook-inbox, сверка неизвестных исходов, снятие поздних и осиротевших hold (по статусу поездки из Trip), удаление токенов у PSP, сверка реестров.

**Owned Data:** `Authorization`, `Payment`, `PaymentAttempt`, `LedgerEntry`, `Debt`, `Fine`, `FinePolicy`, `FineAppeal`, `Refund`, `PspWebhookInbox`, outbox/inbox. Повторные денежные решения исключают уникальные ключи:

- `(trip_id, operation_type, operation_seq)`;
- `(violation_evidence_id, fine_policy_version)`;
- `(provider, provider_event_id)`.

**Storage:** Billing DB (PostgreSQL с синхронной репликой; PITR — 35 сут). Денежные операции, платежи и журнал проводок партиционированы помесячно и хранятся в БД 18 месяцев (окно чарджбэков); затем партиции выгружаются в Parquet в Billing Archive — бакет Billing в Archive Store с versioning и Object Lock (compliance), хранение ≥ 5 лет после отчётного года ([billing-service.md §4.5](services/billing-service.md#45-партиционирование-ретенция-архив)). `psp_webhook_inbox` не партиционирован (нужен глобальный UNIQUE `(provider, provider_event_id)`), обработанные строки удаляются через 14 суток. Карточные реквизиты не хранятся — используются токены PSP. При удалении аккаунта финансовые записи сохраняются на срок, требуемый законом, в псевдонимизированном виде.

**API (кратко):**

- Публичные, `/api/v1`: `payment-methods`, `payments`, `fines`, `fines/{fineId}`, `fines/{fineId}/appeals`, `debts`, `debts/payments`.
- Внутренние (Trip Saga Worker): `POST /internal/v1/payment-authorizations`, `POST …/{authorizationId}/cancel`, `GET …/{authorizationId}` — исход авторизации в `PENDING_RECONCILIATION` Trip узнаёт опросом, события нет.
- Webhook: `POST /webhooks/v1/psp/{provider}` — только через Gateway.
- `/admin/v1/...` — возвраты, платежи, решения по обжалованиям, политики штрафов, `POST /admin/v1/fines` — ручной штраф после разбора (`source = MANUAL`).

**Dependencies:**

- Синхронно:
  - PSP;
  - Pricing — только Billing Settlement Worker (`/internal/v1/price-calculations`, `/internal/v1/signing-keys`);
  - Trip API — Billing Reconciliation Worker, `GET /internal/v1/trips/{tripId}` для осиротевших hold;
  - Photo API — Billing API, `GET /internal/v1/photos/{photoId}` при обжаловании;
  - Geofence API — Billing API, только `GET /internal/v1/violation-evidence/{evidenceId}` при обжаловании (геопроверки Billing не читает: всё нужное приходит в `TripEnded.finishCheck`).
- Событийно — `trip.lifecycle.v1`, `user.access.v1` (Billing Settlement Worker).

**Communication:** публикует `PaymentStatusChanged` в `billing.payment-status.v1` и `FineIssued` / `FineStatusChanged` в `billing.fine.v1`. Производители `billing.fine.v1`: Billing Settlement Worker (`FineIssued` по свидетельству, списание), Billing API (ручной штраф, обжалования), Billing Reconciliation Worker (`FineStatusChanged(PAID)` после сверки с PSP).

## 8. Geofence Service

**Bounded context:** Geofencing. **Владелец:** команда геополитик. **Детально:** [geofence-service.md](services/geofence-service.md).

**Responsibility:** версии зон, проверка старта и парковки, фиксация географических свидетельств нарушения. Денежный штраф создаёт Billing. Непрерывное ограничение скорости исполняется устройством по подписанной политике.

**Containers:** **Geofence API** (worker не нужен). Пространственный индекс (JTS) держится в памяти каждой реплики и обновляется по `geofence.version.v1`.

**Owned Data:** `Geofence`, `GeofenceVersion`, `GeoPolicy`, `GeoCheck`, `ViolationEvidence`. Свидетельство содержит **подписанное Telemetry измерение** и поэтому неотрекаемо.

**Storage:** Geofence DB (PostgreSQL + PostGIS, GiST). `geo_check` партиционирована помесячно.

**API (кратко):**

- Внутренние: `POST /internal/v1/geo-checks/start`, `POST /internal/v1/geo-checks/finish`, `GET /internal/v1/geo-checks/{checkId}` (только Trip Saga Worker — разбор `trip_review`), `GET /internal/v1/geofence-versions/current` (Discovery Projector), `GET /internal/v1/geofence-versions/{versionId}` (Discovery Projector), `GET /internal/v1/violation-evidence/{evidenceId}` (allowlist `billing-api`, обжалование штрафа).
- `/admin/v1/geofences`, `/admin/v1/geofence-versions/...` — черновик, валидация, публикация; `/admin/v1/violation-evidence/{evidenceId}`, `/admin/v1/geo-checks` — разбор и обжалования.

Ответ проверки содержит `decision`, `reasonCode` из словаря Geofence (`OK`, `BOUNDARY_TOLERANCE`, `OUTSIDE_OPERATING_AREA`, `NO_RIDE_ZONE`, `NO_PARKING_ZONE`, `PARKING_OUTSIDE_ZONE`, `MEASUREMENT_STALE`, `LOW_ACCURACY`, `CLOCK_SKEW`) и `policyVersion` (целое, монотонно по `cityId`). По `reasonCode` Billing выбирает политику штрафа.

**Dependencies:**

- Синхронно — JWKS Telemetry (кешируется).
- `LocationObserved` **не потребляет**: Trip передаёт подписанное измерение, полученное через Fleet.

## 9. Photo Service

**Bounded context:** Photo Evidence (поддерживающий). **Владелец:** команда аренды (основной потребитель). **Детально:** [photo-service.md](services/photo-service.md); решение — [ADR-003](adr/ADR-003-photo-evidence.md).

**Responsibility:** приём фото парковки, повреждений и фото к обжалованию штрафа. Сервис отвечает за проверку, превью без EXIF, дедуп и выявление повторного использования, ретенцию и удержания для споров, удаление по сроку и по удалению аккаунта. Бизнес-смысл фото (что поездка принадлежит пользователю) сервис не знает — это проверяет потребитель.

**Containers:**

- **Photo API** — сессии загрузки, presigned URL, метаданные, ссылки просмотра, удержания.
- **Photo Processing Worker** — проверка, превью, pHash, публикация статуса, ретенция, удаление, сверка с объектным хранилищем.

**Owned Data:** `Photo`, `PhotoProcessingJob`, `PhotoRetentionHold`, `PhotoPhash`, `PhotoAccessAudit`; бинарные объекты.

**Storage:** метаданные хранятся в Photo DB (PostgreSQL, помесячные партиции). Бинарные файлы — в Photo Object Storage (S3-совместимое хранилище, SSE-KMS, Object Lock для удержаний, lifecycle). Файлы загружаются напрямую по presigned URL, минуя Gateway.

Ретенция (предлагаемая, [photo-service.md §4.7](services/photo-service.md#47-ретенция-удержания-удаление)): `TRIP_PARKING` — 90 дней, `DAMAGE_REPORT` — 180 дней, `FINE_APPEAL` — 180 дней; фото под удержанием `FINE` / `FINE_APPEAL` / `LEGAL` хранятся до снятия удержания и ещё 3 года; отклонённые — 30 дней; tombstone удалённого фото — 30 дней.

**API (кратко):**

- `POST /api/v1/photos/upload-sessions`, `POST /api/v1/photos/{photoId}/complete`, `GET /api/v1/photos/{photoId}`, `POST /api/v1/photos/{photoId}/view-url`.
- Лимиты загрузки ([photo-service.md §3.2.1](services/photo-service.md#321-post-apiv1photosupload-sessions)): Gateway — 20 запросов `upload-sessions` в минуту на пользователя (`429 RATE_LIMITED`); бизнес-квота Photo — 20 сессий в час и 100 в сутки на пользователя (`429 UPLOAD_RATE_LIMITED`); не более 3 активных фото и 6 сессий на пару (пользователь, субъект) (`409 PHOTO_LIMIT_EXCEEDED`).
- Внутренний: `GET /internal/v1/photos/{photoId}`; allowlist — `trip-saga-worker` (сверка фото парковки), `fleet-api` (фото отчётов о повреждении), `billing-api` (фото обжалования: владелец, `purpose = FINE_APPEAL`, статус `VALIDATED`).
- `/admin/v1/photos/...`.

**Dependencies:** синхронно — объектное хранилище и KMS (SSE-KMS). Событийно потребляет:

- `user.access.v1` — удаление аккаунта;
- `billing.fine.v1` — удержания.

Публикует `PhotoValidated` и `PhotoRejected` в `photo.status.v1`. Для `EXPIRED` и `DELETED` события не публикуются: потребители узнают такой статус через внутренний GET.

## Инфраструктурные компоненты

Следующие компоненты — общая платформа DevOps, а не дополнительные bounded contexts и не владельцы `Trip`/`Payment`:

- API Gateway;
- Kafka и Schema Registry;
- MQTT-брокер (EMQX);
- Outbox CDC (Kafka Connect + Debezium);
- S3-совместимое объектное хранилище (фото; платформенное архивное хранилище Archive Store — архивы Trip, Billing, Fleet, Geofence и журнала расчётов Pricing в Parquet, versioning и Object Lock на срок хранения);
- корпоративный OIDC IdP — внешняя система: аутентификация операторов с MFA, claims `roles[]` и `cityIds[]`; Gateway проверяет операторские JWT по JWKS IdP ([api-conventions.md §2.2](api-conventions.md#22-оператор));
- Vault — секреты сервисов и ключи подписи с ротацией: Transit хранит Ed25519-ключ подписи измерений Telemetry; там же — приватные ключи подписи котировок Pricing и JWT User. Сервисы кешируют ключи в памяти и при кратком отказе Vault продолжают подписывать;
- KMS — шифрование на стороне хранилища (S3 SSE-KMS для фото) и envelope-шифрование ключей;
- балансировщики и наблюдаемость.

Gateway маршрутизирует запросы, проверяет токены (пользовательские — по JWKS User Service, операторские — по JWKS корпоративного IdP) и применяет квоты; saga он не координирует. Отдельные лимиты ресурсов защищают критичные команды от пиков поиска, телеметрии и загрузки фото.
