# Каталог микросервисов

Для всех сервисов: только владелец изменяет свою БД; идентификаторы чужих агрегатов не создают межсервисных SQL-связей. API версионируются, команды принимают ключ идемпотентности, внутренние вызовы аутентифицируются. Перечисленные технологии — проектный выбор; достижение требуемой нагрузки подтверждается измерениями.

Зависимости ниже разделены на **синхронные** и **событийные**: отсутствие прямого RPC не означает отсутствие зависимости от поступления данных. Стрелки публикации и потребления подробно перечислены в отчёте.

## 1. User Service

**Bounded context:** User Management. **Владелец:** команда пользователей.

**Responsibility:** регистрация, профиль, аутентификация, статус аккаунта и право на начало аренды. Финансовые ограничения получает как факты от Billing; прекращение текущей поездки не запрещается из-за долга.

**Owned Data:** `User`, `UserCredential/IdentityLink`, `UserAccessStatus`, `Session`. Чужой долг — локальная проекция, не финансовый источник истины.

**Storage:** собственная PostgreSQL БД; кеш профиля/сессий. При внешнем IdP пароли и первичная аутентификация делегируются ему через адаптер.

**API:**

- `POST /users`, `GET /users/{userId}`, `PATCH /users/{userId}`.
- `POST /auth/token`, `POST /auth/refresh`.
- `GET /users/{userId}/rental-eligibility` — внутреннее API права на старт.

**Dependencies:** синхронно — только IdP, если подключён; событийно — Billing (`PaymentStatusChanged`, `FineIssued` и факты погашения в платёжном статусе).

**Communication:** HTTPS/REST; публикует `UserAccessChanged`, потребляет финансовые статусы Kafka. Клиентские JWT проверяются Gateway/сервисами локально по кешируемым публичным ключам, а бизнес-право старта проверяет Trip.

**Scalability:** stateless API, кеши и индексы по `userId`; отдельные лимиты на вход/регистрацию. 5 млн профилей сами по себе не требуют шардирования.

## 2. Fleet Service

**Bounded context:** Fleet & IoT. **Владелец:** команда IoT/парка.

**Responsibility:** учёт самокатов, техническая готовность, исключительное выделение самоката поездке, команды замку и сверка с устройством. Состояние поездки и её цена сервису не принадлежат.

**Owned Data:** `Scooter`, `ScooterLease`, `DeviceCommand`, `DeviceState`, outbox. Последняя телеметрия — проекция данных Telemetry. `ScooterLease` содержит `tripId`, фазу и fencing token; условное обновление/уникальность в БД запрещает двух владельцев.

**Storage:** собственная PostgreSQL БД; стабильное состояние lease и команд не хранится только в Redis.

**API:**

- `GET /fleet/scooters/{scooterId}` — состояние, последняя доверенная координата и свежесть.
- `POST /fleet/scooters/{scooterId}/leases` — захват для `tripId`.
- `POST /fleet/leases/{leaseId}/release` — освобождение с проверкой владельца и состояния замка.
- `POST /fleet/scooters/{scooterId}/commands` — `UNLOCK`, `LOCK`, `QUERY_STATE` с `commandId`, lease и сроком действия.
- `GET /fleet/commands/{commandId}` — статус для сверки, независимо от Kafka.
- `PATCH /fleet/scooters/{scooterId}/maintenance` — операторская блокировка.

**Dependencies:** синхронно/по командам — MQTT и устройство; событийно — Telemetry для измерений и свежести. Сервис не вызывает Trip, чтобы решить, свободен ли самокат.

**Communication:** REST для Trip/операторов; MQTT с устройством; Kafka `ScooterAvailabilityChanged`, `DeviceCommandUpdated`. ACK MQTT подтверждает доставку сообщения, а бизнес-подтверждение устройства — фактическое исполнение; это разные факты.

**Scalability:** независимые API и workers соединений/команд; маршрутизация по `scooterId`. Один агрегат сериализуется в БД; подготовительный TTL не освобождает самокат после команды с неизвестным исходом.

## 3. Telemetry Service

**Bounded context:** Fleet & IoT. **Владелец:** команда IoT/парка.

**Responsibility:** приём, аутентификация и нормализация GPS/заряда/heartbeat, отбраковка дублей и явно некорректных измерений. Сервис не назначает поездку и не разрешает разблокировку.

**Owned Data:** `Location`, `TelemetrySample`, `DeviceHeartbeat`, идентификаторы/последовательности измерений и метаданные качества. Учётная запись самоката остаётся во Fleet.

**Storage:** отдельное партиционированное time-series хранилище, например PostgreSQL/TimescaleDB; архивирование и агрегация по ретенции. Права/сертификаты устройств выдаются инфраструктурой IoT, логическая привязка устройства принадлежит Fleet.

**API:**

- `GET /telemetry/scooters/{scooterId}/location` — внутреннее чтение последнего измерения.
- `GET /telemetry/scooters/{scooterId}/locations?from=...&to=...` — доступная по ролям история.
- MQTT `devices/{scooterId}/telemetry` — основной вход устройства.

**Dependencies:** MQTT-брокер, собственная БД и Kafka; для принятия каждого измерения не требуется RPC в Fleet.

**Communication:** MQTT на входе, Kafka `LocationObserved` для Discovery/Fleet/Geofence, REST для служебного чтения. Идентификатор измерения обеспечивает повторы; публикация следует за устойчивой записью.

**Scalability:** партиционирование по времени и `scooterId`, пакетные записи, отдельные consumer groups и квоты. Число workers ограничено партициями; нагрузка определяется устройствами и интервалом измерений, а не RPS мобильного API.

## 4. Discovery Service

**Bounded context:** Discovery. **Владелец:** команда поиска/карты.

**Responsibility:** поиск ближайших самокатов, фильтрация доступности и отображение зон/ориентировочной цены. Поиск не резервирует ресурс.

**Owned Data:** `ScooterSearchDocument`, географический индекс, отметки свежести, версии обработанных событий. Это принадлежащая сервису **производная проекция**; владельцы `Scooter`, `Location`, `Tariff`, `Geofence` остаются в исходных сервисах.

**Storage:** Redis GEO/геоиндекс, региональные кеши и checkpoint потребителей. Индекс восстанавливается из согласованных snapshot исходных данных и последующих событий; одной ограниченной ретенции Kafka может быть недостаточно.

**API:**

- `GET /scooters/nearby?lat=...&lon=...&radius=...`.
- `GET /scooters/{scooterId}/card`.
- `GET /map/geofences?bbox=...`.

**Dependencies:** без синхронного fan-out в обычном поисковом запросе; событийно — Fleet, Telemetry, Pricing, Geofence. Служебные snapshot API этих владельцев требуются для восстановления индекса.

**Communication:** REST для клиентов; потребление событий Kafka. При задержке данных скрывает устаревших кандидатов и сообщает время обновления.

**Scalability:** больше read-реплик/шардов, разбиение по городам и геоячейкам, кеширование популярных областей с ограниченным TTL. Сбой Discovery не должен занимать пул ресурсов Trip/Fleet.

## 5. Trip Service

**Bounded context:** Rental. **Владелец:** команда аренды.

**Responsibility:** создание и жизненный цикл поездки, оркестрация проверок, авторизации и разблокировки, безопасное завершение, восстановление незавершённых операций. Разблокировка — шаг durable saga этого сервиса.

**Owned Data:** `Trip`, `TripStateTransition`, `TripStartOperation`, `TripFinishOperation`, `SagaStep`, `IdempotencyRecord`, outbox/inbox. Snapshot тарифа, проверок зоны и платёжных статусов — факты/копии, не право изменять исходные сущности.

**Storage:** собственная PostgreSQL БД. Частичный уникальный индекс по `userId` для незавершённых поездок обеспечивает инвариант в одной транзакции.

**API:**

- `POST /trips` — начать, `202` и ссылка на ресурс операции.
- `GET /trips/{tripId}`, `GET /users/{userId}/trips`.
- `POST /trips/{tripId}/finish` — принять намерение завершить.
- `GET /trips/{tripId}/operations/{operationId}`.

**Dependencies:** синхронно — User (новый старт), Fleet, Pricing (котировка), Geofence, Billing (авторизация). Асинхронно — результаты команд Fleet, ограничения User и финансовые статусы Billing. Завершение не требует ответа Billing или Pricing.

**Communication:** REST-команды и ограниченные по времени RPC; Kafka для `TripEnded` и обновлений проекций; polling Fleet как путь сверки при задержке событий. Команды устройства отправляет только Fleet.

**Scalability:** stateless API, горизонтальные saga workers, выдача работы с блокировкой/lease в БД, индексы истории по пользователю. Шардирование обсуждается по результатам измерений; инвариант пользователя должен сохраняться в одном разделе.

## 6. Pricing Service

**Bounded context:** Pricing. **Владелец:** команда тарификации.

**Responsibility:** версии тарифов, котировки, детерминированный расчёт цены поездки по зафиксированным условиям. Сервис не списывает деньги и не принимает решение о виновности пользователя.

**Owned Data:** `Tariff`, `TariffVersion`, `PriceRule`, `Quote` с TTL, версия алгоритма расчёта. Активированные версии не изменяются задним числом.

**Storage:** собственная PostgreSQL БД; кеш неизменяемых опубликованных версий.

**API:**

- `GET /tariffs?cityId=...`, `POST /tariffs`, `POST /tariffs/{tariffId}/versions` — часть операций доступна только операторам.
- `POST /quotes` — полный snapshot, валюта, алгоритм, подпись/хеш и срок принятия.
- `POST /price-calculations` — расчёт по snapshot, времени и подтверждённым параметрам поездки; повтор даёт тот же результат.

**Dependencies:** синхронные бизнес-зависимости отсутствуют. Trip передаёт контекст/параметры; Billing передаёт сохранённый snapshot и факты поездки. Скрытых чтений чужих БД нет.

**Communication:** REST; Kafka `TariffVersionPublished`. При недоступности сервиса ранее принятая поездка не теряет условия; отложенный расчёт Billing возобновляется позже.

**Scalability:** read-реплики/кеши, горизонтальные вычислители; старые версии алгоритмов доступны для воспроизведения исторического расчёта.

## 7. Billing Service

**Bounded context:** Billing. **Владелец:** команда расчётов.

**Responsibility:** авторизация, списание, возврат, финансовый учёт, долг и штрафы. Самокат не остаётся «в поездке» из-за неуспешного списания.

**Owned Data:** `Payment`, `PaymentAttempt`, `Authorization`, `LedgerEntry`, `Fine`, `FinePolicy`, `FineAssessment`, `Refund`, `PaymentProviderReference`, outbox/inbox. Уникальность `tripId + operationType + operationSequence` и `violationEvidenceId + finePolicyVersion` предотвращает повторные денежные решения.

**Storage:** собственная PostgreSQL БД с транзакционным журналом операций. Карточные реквизиты не хранятся, используются токены PSP. Прошлые проводки корректируются новыми записями, а не переписываются.

**API:**

- `POST /payment-authorizations`, `POST /payment-authorizations/{id}/cancel`.
- `GET /payments/{paymentId}`, `GET /users/{userId}/payments`.
- `POST /payments/{paymentId}/refunds` — контролируемый доступ.
- `GET /users/{userId}/fines`, `POST /fines/{fineId}/appeals`.
- `POST /billing/provider-webhooks` — проверяемая подпись, защита от повтора, сверка статуса.

**Dependencies:** синхронно — PSP, Pricing для окончательного расчёта; событийно — Trip (`TripEnded`, включая snapshot и доказательства нарушений). Финансовое решение не строится по данным поискового индекса.

**Communication:** REST, подписанные webhook PSP, Kafka `PaymentStatusChanged`, `FineIssued`. PSP-интеграция закрыта ACL; timeout не равен отказу платежа. Повторный webhook не создаёт повторную проводку.

**Scalability:** отдельно API авторизации, очереди расчётов и webhook workers; лимиты PSP, backpressure, партиционирование по `paymentId/tripId`, транзакционная дедупликация. Реплики API не снимают ограничений пропускной способности PSP.

## 8. Geofence Service

**Bounded context:** Geofencing. **Владелец:** команда геополитик.

**Responsibility:** версии зон, проверка старта/парковки, правила ограничения движения и фиксация географических свидетельств. Итоговый денежный штраф создаёт Billing. Непрерывное ограничение скорости, требующее реакции без сети, должно исполняться локально устройством по подписанной политике; серверные события не являются контуром физической безопасности в реальном времени.

**Owned Data:** `Geofence`, `GeofenceVersion`, `GeoPolicy`, `GeoCheck`, `ViolationEvidence`. Доказательство содержит координату, точность, время, версию зоны и результат; это snapshot наблюдения Telemetry, а не второй владелец истории GPS.

**Storage:** собственная PostgreSQL/PostGIS БД, пространственные индексы и локальные версионные кеши правил.

**API:**

- `GET /geofences?bbox=...`, `POST /geofences`, `POST /geofences/{id}/versions`.
- `POST /geo-checks/start`, `POST /geo-checks/finish` — координата/measurementId, точность, время; ответ содержит `allowed`, причину, `policyVersion`, свидетельство при наличии.
- `GET /geo-checks/{checkId}` — воспроизводимый результат проверки.

**Dependencies:** синхронных вызовов других бизнес-сервисов для проверки нет; событийно — Telemetry для наблюдений. При запросе Trip передаёт доверенное серверное измерение, проверяемое по идентификатору/проекции.

**Communication:** REST; Kafka `GeofenceVersionPublished`, потребление `LocationObserved`. В этой версии автоматический штраф по одному потоку GPS не создаётся: Trip прикладывает доказательство проверки завершения к `TripEnded`.

**Scalability:** горизонтальные API с локальными индексами опубликованных зон; разбиение по городам. Публикация новой версии атомарно меняет указатель версии, сохраняя прошлые проверки.

## Инфраструктурные компоненты

API Gateway, Kafka, MQTT, балансировщики, наблюдаемость и секреты — общая платформа DevOps, а не дополнительные bounded contexts или владельцы `Trip`/`Payment`. Gateway маршрутизирует, проверяет токены и применяет квоты; он не координирует saga. Отдельные лимиты ресурсов защищают критичные команды от пика поиска и телеметрии.
