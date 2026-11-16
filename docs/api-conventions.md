# Конвенции API и событий

Общие правила для всех девяти сервисов: классы API, аутентификация и авторизация, заголовки, версионирование, форматы, ошибки, идемпотентность, конкурентный доступ, пагинация, лимиты, устойчивость межсервисных вызовов, трассировка, маршруты Gateway, MQTT и события Kafka. Конкретные эндпоинты, поля и коды ошибок описывает сервис-владелец в разделе «3. API» своего документа ([ссылки](#16-документы-сервисов)); при расхождении деталей эндпоинта приоритет у документа владельца, при расхождении общих правил — у этого документа.

Контекст: [отчёт lab-01](lab-01.md) (§5.4 — контракты событий и надёжность доставки, §6 — нагрузка и отказы), [каталог сервисов](service-catalog.md), [хранение данных](data-storage.md).

## 1. Классы API и пути

| Класс | Префикс | Кто вызывает | Через что | Аутентификация | Пример |
| --- | --- | --- | --- | --- | --- |
| public | `/api/v1/...` | Мобильное приложение пользователя | API Gateway | JWT пользователя | `POST /api/v1/trips` |
| admin | `/admin/v1/...` | Операторская консоль | API Gateway | JWT оператора с ролями | `POST /admin/v1/geofence-versions/{versionId}/publish` |
| internal | `/internal/v1/...` | Контейнеры платформы | Напрямую по внутренней сети; Gateway **не** маршрутизирует | mTLS + сервисная идентичность + allowlist вызывающих | `POST /internal/v1/geo-checks/finish` |
| webhook | `/webhooks/v1/psp/{provider}` | Платёжный провайдер | API Gateway (отдельный маршрут без JWT) | Подпись провайдера + IP allowlist | `POST /webhooks/v1/psp/acquirer-a` |
| служебные | `/.well-known/jwks.json`, `/actuator/health/{liveness,readiness}` | Gateway и сервисы; Kubernetes | Внутренняя сеть | Без аутентификации, только внутри кластера | — |

Правила путей:

- Ресурсы — существительные во множественном числе, kebab-case: `/payment-methods`, `/geofence-versions`. Идентификаторы — в пути: `/trips/{tripId}`.
- Переход состояния, который не сводится к изменению полей, — POST на подресурс-действие: `/trips/{tripId}/finish`, `/geofence-versions/{versionId}/publish`, `/payment-authorizations/{id}/cancel`. Создание — `POST` на коллекцию (`201` + `Location`), длительная команда — `202` + ссылка на операцию.
- Вложенность — не глубже двух уровней (`/trips/{tripId}/operations/{operationId}`).
- Параметры запроса — camelCase (`cityId`, `radiusM`).
- Внутренний API сервиса не дублирует публичный: публичные чтения идут через Gateway в API сервиса, а не в чужой `/internal`.

## 2. Аутентификация и авторизация

### 2.1. Пользователь

- **Выдача.** User API выдаёт пару токенов (`POST /api/v1/auth/token`, `grantType: otp|refresh_token`):
  - `accessToken` — JWT `ES256`, TTL 10 мин;
  - `refreshToken` — непрозрачный, TTL 30 дней, одноразовый с ротацией. Повторное использование отозванного refresh-токена отзывает всё семейство сессии (признак кражи).
- **Claims** `accessToken` (детали — [user-service.md](services/user-service.md#3-api)):
  - `iss: "user-service"`, `aud: "scooter-platform"`;
  - `sub` = `userId`, `roles: ["RIDER"]`;
  - `sid` (сессия), `jti`, `iat`, `exp`, `auth_time`.
  - Права на поездку в токен **не** кладутся: блокировка и долг проверяются по бизнес-операции (`GET /internal/v1/users/{userId}/rental-eligibility`), поэтому действуют сразу, а не после истечения токена.
- **Проверка.** Gateway проверяет подпись по `GET /.well-known/jwks.json` User API (кеш 5 мин, внеплановое обновление при неизвестном `kid`), а также `exp`, `aud` и `iss`. Новый ключ подписи появляется в JWKS за 24 ч до использования, старый остаётся в JWKS 24 ч после вывода. Gateway передаёт исходный `Authorization` дальше, и сервис повторно проверяет токен тем же JWKS: zero trust, проверка дешёвая благодаря кешу.
- **Идентичность берётся только из `sub`.** `userId` из тела или query публичного запроса не принимается: это защита от подмены идентичности и IDOR.
- **Доступ к ресурсу проверяет сервис-владелец** (`trip.userId == sub`). На чужой ресурс отвечаем `404 NOT_FOUND`, а не `403`, чтобы не раскрывать, что ресурс существует.
- **Выход и блокировка** отзывают refresh-токены сессии. Уже выданный access-токен живёт не дольше 10 мин, а критичные операции всё равно проверяют право.

### 2.2. Оператор

- **Выдача.** Токен выдаёт корпоративный OIDC IdP с обязательным MFA (Authorization Code + PKCE в операторской консоли). Это внешняя для платформы система (`System_Ext` «Корпоративный OIDC IdP» на диаграммах [C1](../diagrams/c4-context.puml) и [C2](../diagrams/c4-containers.puml)): оператор входит в IdP, а API Gateway проверяет его JWT по JWKS IdP. Роли и `cityIds` назначаются в IdP.
- **Claims:**
  - `sub` = `operatorId`;
  - `roles[]` ⊂ {`OPERATOR`, `SUPPORT`, `FINANCE`, `PRICING_MANAGER`, `GEO_MANAGER`, `ADMIN`};
  - `cityIds[]` — города, которыми оператор может управлять (пусто у `ADMIN` = все);
  - `amr` содержит `mfa`;
  - TTL 10 мин.
- **Проверка.** Gateway проверяет JWT по JWKS IdP и наличие роли для области пути. Сервис проверяет роль для конкретного действия и `cityId` ресурса ∈ `cityIds`.
- **Аудит.** Любое изменение и любой просмотр персональных или доказательных данных записываются в журнал аудита сервиса. Это касается истории перемещений, свидетельств нарушений, фото, карточки пользователя. Запись содержит: `operatorId`, роли, действие, объект, `reason`, `traceId`. Для таких чтений параметр `reason` обязателен.
- **Параметр `reason`.** Причина доступа передаётся параметром запроса `reason` у `GET`; у админ-команд, где документ сервиса описывает тело запроса, — полем `reason` в теле. Отдельного заголовка (вроде `X-Access-Reason`) нет. Значение — непустая строка ≤ 500 символов (номер тикета или пояснение), пишется в журнал аудита. Владелец может сузить формат: Telemetry принимает перечисление причин, Photo — `reasonCode` из перечисления и `reasonText`. Нет или пуст обязательный `reason` → `400 REASON_REQUIRED` ([раздел 6](#6-ошибки-rfc-9457)).

Базовая матрица ролей приведена ниже; точные роли каждого эндпоинта задаёт документ сервиса. Владелец может сузить доступ, расширение требует правки матрицы.

| Область `/admin/v1/...` | Сервис | Чтение | Изменение |
| --- | --- | --- | --- |
| `users/**` | User | `SUPPORT`, `OPERATOR`; поиск по телефону — `SUPPORT` (с `reason`) | блокировки и отзыв сессий: `SUPPORT` |
| `scooters/**`, `damage-reports/**` | Fleet | `OPERATOR`, `SUPPORT` | `OPERATOR` (команды `LOCK`/`QUERY_STATE`, обслуживание, отчёты); регистрация самоката и сервисный `UNLOCK` — только `ADMIN` |
| `leases/**` | Fleet | — | принудительное освобождение lease (`force-release`): только `ADMIN` |
| `telemetry/**` | Telemetry | `OPERATOR`, `SUPPORT` (с `reason`, аудит); журнал доступа `access-audit` — только `ADMIN` | — |
| `discovery/**` | Discovery | `OPERATOR` | перестроение: `OPERATOR` |
| `trips/**` | Trip | `SUPPORT`, `OPERATOR`, `FINANCE` | принудительное завершение: `SUPPORT`; решения по сверке: `SUPPORT`, `OPERATOR`; повтор шага saga: только `ADMIN` |
| `trip-reviews/**` | Trip | `OPERATOR`, `SUPPORT` | решения по разбору: `OPERATOR`, `SUPPORT` |
| `tariffs/**` | Pricing | `PRICING_MANAGER`, `FINANCE`, `SUPPORT` | `PRICING_MANAGER` |
| `price-calculations` | Pricing | `FINANCE`, `SUPPORT` | — |
| `payments/**` | Billing | `FINANCE`, `SUPPORT` | возвраты: `FINANCE` |
| `fines/**`, `fine-policies/**` | Billing | штрафы: `OPERATOR`, `SUPPORT`, `FINANCE`; политики: `FINANCE`, `SUPPORT` | решения по обжалованиям: `SUPPORT`, `FINANCE`; ручной штраф (`POST /admin/v1/fines`): `FINANCE`; политики штрафов: `FINANCE` |
| `geofences/**` | Geofence | `GEO_MANAGER`, `OPERATOR`, `SUPPORT` | — |
| `geofence-versions/**` | Geofence | `GEO_MANAGER` | `GEO_MANAGER` |
| `geo-checks` | Geofence | `SUPPORT`, `OPERATOR` | — |
| `violation-evidence/**` | Geofence | `SUPPORT`, `FINANCE`, `GEO_MANAGER` (с `reason`, аудит) | — |
| `photos/**` | Photo | поиск: `OPERATOR`, `SUPPORT`, `FINANCE`; ссылка просмотра (с `reason`, аудит): `PREVIEW` — `OPERATOR`, `SUPPORT`, `FINANCE`, `ORIGINAL` (с EXIF) — только `SUPPORT`, `FINANCE` | удержания: `SUPPORT`, `FINANCE`; снятие ручного удержания, досрочное удаление и повторная обработка — только `ADMIN`. `cityIds` оператора Photo не проверяет: города в модели фото нет ([photo-service.md](services/photo-service.md#3-api)) |

`ADMIN` имеет все права. Действия `ADMIN` в обход правила «четырёх глаз» (публикация собственного черновика тарифа или зон) требуют `reason`. Назначение ролей выполняется в IdP, а не в сервисах.

### 2.3. Сервис-сервис (`/internal`)

- Внутренние вызовы идут по mTLS, TLS 1.3. Идентичность нагрузки — X.509 SVID с SAN `spiffe://scooter.internal/ns/<namespace>/sa/<container-name>`. Сертификат выдаёт cert-manager или mesh, TTL 24 ч, ротация автоматическая. `<container-name>` — kebab-имя контейнера из [15.6](#156-именование), например `trip-saga-worker`.
- Для каждого внутреннего эндпоинта задаётся **allowlist вызывающих** по имени контейнера; он описан в документе владельца. Вызывающий вне списка получает `403 CALLER_NOT_ALLOWED`. Дополнительно действует сетевая политика Kubernetes: «кто к кому может подключаться».
- JWT пользователя во внутренние вызовы не передаётся. Если операции нужен пользователь, доверенный вызывающий передаёт `userId` в теле: эндпоинт доступен только контейнерам из allowlist, а не клиенту.
- Gateway не публикует `/internal/**`: такой путь на Gateway даёт `404`.

### 2.4. Webhook PSP

- `POST /webhooks/v1/psp/{provider}` идёт через Gateway по отдельному маршруту без JWT, с IP allowlist провайдера и лимитом размера тела 64 КБ.
- Billing API проверяет подпись провайдера по **сырому** телу и метке времени (схема провайдера, например HMAC-SHA256). Допустимое расхождение времени — ±5 мин. Неверная подпись → `401`, тело не сохраняется.
- Дедупликация — по `(provider, providerEventId)` (уникальный ключ webhook-inbox). Ответ `200` отдаётся сразу после надёжной записи в inbox, обработку выполняет Billing Reconciliation Worker. Повтор того же события снова получает `200` без повторной обработки.
- Содержимое webhook — сигнал, а не источник истины для денег. Спорный или неожиданный статус подтверждается запросом к API PSP; подробности в [billing-service.md](services/billing-service.md#3-api).

## 3. Заголовки

| Заголовок | Направление | Где | Правило |
| --- | --- | --- | --- |
| `Authorization: Bearer <JWT>` | запрос | public, admin | Обязателен, кроме `/api/v1/auth/otp`, `/api/v1/auth/token` и webhook |
| `Idempotency-Key` | запрос | POST-команды public и admin; internal без естественного ключа | 8–64 символа `[A-Za-z0-9_-]`, рекомендуется UUID. Алгоритм — [раздел 7](#7-идемпотентность-команд) |
| `Idempotency-Replayed: true` | ответ | все идемпотентные команды | Ответ — повтор сохранённого результата |
| `traceparent`, `tracestate` | запрос/ответ | все | W3C Trace Context, [раздел 12](#12-трассировка-и-корреляция) |
| `X-Request-Id` | запрос/ответ | все | UUIDv7, генерирует Gateway (или вызывающий сервис), возвращается в ответе, пишется в логи |
| `If-Match` | запрос | PATCH и переходы состояния версионируемых ресурсов | Без него → `428 PRECONDITION_REQUIRED`; устарел → `412 PRECONDITION_FAILED` ([раздел 8](#8-конкурентный-доступ-etag-и-if-match)) |
| `ETag` | ответ | GET версионируемых и неизменяемых ресурсов | Сильный ETag: `"<row_version>"` или `"sha256:<checksum>"` |
| `If-None-Match` | запрос | условный GET | Совпал → `304 Not Modified` |
| `Location` | ответ | `201`, `202` | Ссылка на созданный ресурс или операцию (`/api/v1/trips/{tripId}/operations/{operationId}`) |
| `Retry-After` | ответ | `429`, `503`, `409 IDEMPOTENCY_IN_PROGRESS`, `202` | Секунды; для `202` — рекомендуемый интервал опроса |
| `RateLimit-Policy`, `RateLimit` | ответ | public | Политика и остаток лимита (IETF draft), [раздел 10](#10-лимиты-запросов-на-gateway) |
| `Deprecation`, `Sunset`, `Link: <...>; rel="successor-version"` | ответ | устаревшие версии | [Раздел 4](#4-версионирование) |
| `Content-Type` | оба | все | `application/json; charset=utf-8`; ошибки — `application/problem+json`; PATCH — `application/merge-patch+json`; GeoJSON — `application/geo+json` |
| `Accept-Language` | запрос | public, admin | `ru` (по умолчанию) или `en` — язык `title` и `detail` ошибок; `code` не локализуется |
| `Accept-Encoding: gzip` | запрос | все | Ответы > 1 КБ сжимаются |

Gateway удаляет из входящих запросов заголовки, которые клиент подделывать не должен: `X-Forwarded-*` выставляет сам, внутренние `X-Internal-*` удаляет.

## 4. Версионирование

- **Major — в пути** (`/api/v1`, `/admin/v1`, `/internal/v1`). Обратно совместимые изменения вносятся без смены версии, только аддитивно:
  - новый эндпоинт;
  - новое необязательное поле запроса;
  - новое поле ответа;
  - новый код ошибки для новой ситуации;
  - новое значение enum в ответе, если поле помечено как расширяемое.
- **Ломающие изменения требуют нового major:**
  - удаление или переименование поля;
  - смена типа, единиц или смысла поля;
  - перевод поля из необязательных в обязательные;
  - новое обязательное поле запроса;
  - сужение допустимых значений;
  - смена кода ошибки или HTTP-статуса для прежней ситуации;
  - смена поведения по умолчанию.
- **Tolerant reader.** Клиенты игнорируют неизвестные поля ответа и обрабатывают неизвестные значения enum как «прочее». Сервер игнорирует неизвестные поля запроса и считает их в метрике. Ответы не зависят от порядка полей.
- **Срок поддержки старой версии:**

| Класс | Срок после выхода новой major | Механизм |
| --- | --- | --- |
| public | ≥ 12 месяцев: мобильные клиенты обновляются медленно | `Deprecation`/`Sunset`, мониторинг доли запросов по `User-Agent` (`ScooterApp/<version> (<platform>)`), минимальная поддерживаемая версия приложения в конфигурации клиента |
| admin | ≥ 3 месяцев | Одна операторская консоль, обновляется вместе с релизом |
| internal | До перехода всех вызывающих, но не меньше 1 месяца | Обе версии обслуживаются одним развёртыванием; consumer-driven контрактные тесты (Pact) в CI владельца; проверка ломающих изменений OpenAPI (`oasdiff`) |

- У каждого сервиса есть спецификация OpenAPI 3.1 в репозитории сервиса; линтер (Spectral) проверяет её на соответствие этому документу.
- Версионирование событий описано в [15.2](#152-schema-registry-и-эволюция-схем).

## 5. Форматы данных

| Что | Формат | Пример |
| --- | --- | --- |
| Идентификаторы | UUIDv7, строка в нижнем регистре (36 символов). Внешние идентификаторы (PSP) — строка как есть | `"01a1ab15-7700-7c10-8d20-3e4f5a6b7c8d"` |
| Время | RFC 3339, UTC, суффикс `Z`, миллисекунды. Локальное время сервер не отдаёт | `"2026-11-05T09:41:11.020Z"` |
| Дата без времени | `YYYY-MM-DD` — только когда смысл действительно дата | `"2031-11-05"` |
| Длительность | Целое с суффиксом единицы в имени: `...Sec`, `...Ms`, `...Minutes` | `"durationSec": 1520` |
| Деньги | `{amountMinor: int64, currency: ISO 4217}`; в плоских структурах — `<name>Minor` + общее поле `currency`. Плавающая точка запрещена | `{"amountMinor": 18950, "currency": "RUB"}` |
| Координаты | WGS84 (EPSG:4326), `lat`/`lon` — числа, ≤ 7 знаков после запятой (~1 см) | `"lat": 59.9341208, "lon": 30.3352119` |
| Геометрия | GeoJSON (RFC 7946), порядок `[lon, lat]` | `{"type": "MultiPolygon", "coordinates": [...]}` |
| Прямоугольник | `bbox=minLon,minLat,maxLon,maxLat` | `bbox=30.30,59.92,30.36,59.95` |
| Физические величины | Суффикс единицы: `...M` (метры), `...Kmh`, `...Percent` (целое 0–100) | `"accuracyM": 4.0`, `"speedLimitKmh": 15` |
| Перечисления | UPPER_SNAKE_CASE | `"ALLOWED_WITH_VIOLATION"` |
| Имена полей | camelCase | `finishOperationId` |
| Телефон | E.164 | `"+79991234567"` |
| Хеши, подписи | SHA-256 в ответах и событиях — hex в нижнем регистре (64 символа, например поле `sha256` фото) или с префиксом `sha256:`. Base64 — только там, где его требует внешний протокол: заголовок `x-amz-checksum-sha256` presigned-загрузки в объектное хранилище; поэтому клиент передаёт в `POST /api/v1/photos/upload-sessions` поле `sha256Base64`, которое Photo API подставляет в этот заголовок. Подписи — base64url без `=` | `"sha256": "10006e34…"`, `"sha256:9f2c…"` |
| `null` и отсутствие поля | В ответе поля, допускающие `null` (`...\|null` в контракте), возвращаются явно. В PATCH (JSON Merge Patch) отсутствие поля — «не менять», `null` — «очистить» | — |
| Коллекции | Пустой массив, никогда не `null` | `"flags": []` |

## 6. Ошибки (RFC 9457)

Все ошибки возвращаются как `application/problem+json`.

| Поле | Тип | Смысл |
| --- | --- | --- |
| `type` | URI | `https://api.scooter-rent.example/problems/<code-in-kebab-case>` — страница описания ошибки |
| `title` | string | Короткое описание класса ошибки (локализуется) |
| `status` | int | HTTP-статус |
| `detail` | string | Пояснение для этого случая; без стеков, SQL и внутренних имён |
| `instance` | string | Путь запроса |
| `code` | string | Машинный код UPPER_SNAKE — **часть контракта** |
| `traceId` | string | `trace-id` из `traceparent` |
| `errors[]` | array | Ошибки полей: `{field: JSON Pointer, code, message}` |
| расширения | — | Допустимы поля, нужные клиенту для реакции: `retryAfterSec`, `activeTripId` и т. п. (описывает владелец) |

```json
{
  "type": "https://api.scooter-rent.example/problems/validation-failed",
  "title": "Запрос не прошёл проверку",
  "status": 400,
  "detail": "Поле radiusM должно быть в диапазоне 50..2000.",
  "instance": "/api/v1/scooters/nearby",
  "code": "VALIDATION_FAILED",
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "errors": [
    { "field": "/radiusM", "code": "OUT_OF_RANGE", "message": "Допустимо 50..2000" }
  ]
}
```

```json
{
  "type": "https://api.scooter-rent.example/problems/idempotency-key-reused",
  "title": "Ключ идемпотентности использован с другим запросом",
  "status": 422,
  "detail": "Idempotency-Key уже использован для запроса с другим телом. Для новой операции нужен новый ключ.",
  "instance": "/api/v1/trips",
  "code": "IDEMPOTENCY_KEY_REUSED",
  "traceId": "0af7651916cd43dd8448eb211c80319c"
}
```

Правила:

- 4xx означает, что клиент должен изменить запрос или подождать; повтор того же запроса не поможет (кроме `409 IDEMPOTENCY_IN_PROGRESS` и `429`). 5xx и `503` можно повторить по правилам [раздела 11](#11-межсервисные-вызовы-тайм-ауты-повторы-circuit-breaker).
- `409` — состояние ресурса не допускает действие (`ACTIVE_TRIP_EXISTS`, `TRIP_NOT_ACTIVE`, `SCOOTER_LEASED`). `422` — запрос корректен по схеме, но нарушает бизнес-правило, а также `IDEMPOTENCY_KEY_REUSED`. `400` — нарушена схема или формат.
- `VALIDATION_FAILED` — только `400` (схема, формат, диапазон). Нарушение бизнес-правила — `422` с доменным кодом (например, `TARIFF_RULE_VIOLATION`, `EXPIRES_AT_IN_PAST`, `EFFECTIVE_FROM_IN_PAST`), а не `422 VALIDATION_FAILED`.
- Для общих ситуаций используются только коды из таблицы ниже: `UNAUTHENTICATED`, а не `UNAUTHORIZED`; `FORBIDDEN`, а не `ACCESS_DENIED`; `DEPENDENCY_UNAVAILABLE`, а не `SERVICE_UNAVAILABLE`; `IDEMPOTENCY_KEY_REQUIRED` и `REASON_REQUIRED` не заменяются на `VALIDATION_FAILED`.
- Новый `code` — аддитивное изменение; смена `code` для прежней ситуации — ломающее. Клиент, не знающий `code`, реагирует по HTTP-статусу.
- Доменные коды определяет владелец в своём документе (например, `PHOTO_REQUIRED`, `MEASUREMENT_STALE`, `LEASE_FENCED`, `CITY_NOT_CONFIGURED`).

Общие коды:

| `code` | HTTP | Когда | Действие клиента |
| --- | --- | --- | --- |
| `VALIDATION_FAILED` | 400 | Нарушены схема, формат или диапазон | Исправить запрос |
| `INVALID_CURSOR` | 400 | Курсор повреждён или выдан для других фильтров | Начать с первой страницы |
| `IDEMPOTENCY_KEY_REQUIRED` | 400 | POST-команда без `Idempotency-Key` | Добавить ключ |
| `REASON_REQUIRED` | 400 | Нет обязательной причины доступа (`reason`) для аудируемого чтения или команды ([2.2](#22-оператор)) | Указать причину |
| `UNAUTHENTICATED` | 401 | Нет токена, он просрочен или неверен | Обновить токен, повторить |
| `FORBIDDEN` | 403 | Нет роли или города для действия | — |
| `CALLER_NOT_ALLOWED` | 403 | Внутренний вызывающий не в allowlist | Ошибка конфигурации, алерт |
| `NOT_FOUND` | 404 | Ресурса нет или он чужой | — |
| `IDEMPOTENCY_IN_PROGRESS` | 409 | Запрос с тем же ключом ещё выполняется | Повторить с тем же ключом через `Retry-After` |
| `PRECONDITION_FAILED` | 412 | `If-Match` не совпал с текущей версией | Перечитать ресурс и повторить изменение |
| `PAYLOAD_TOO_LARGE` | 413 | Тело больше лимита маршрута | — |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | Неверный `Content-Type` | — |
| `IDEMPOTENCY_KEY_REUSED` | 422 | Тот же ключ с другим телом или параметрами | Новый ключ для новой операции |
| `PRECONDITION_REQUIRED` | 428 | Нет обязательного `If-Match` | Добавить `If-Match` |
| `RATE_LIMITED` | 429 | Превышен лимит | Ждать `Retry-After` |
| `INTERNAL_ERROR` | 500 | Непредвиденная ошибка | Повторять только идемпотентные запросы |
| `UPSTREAM_TIMEOUT` | 504 | Gateway не дождался сервиса | Повторить с тем же `Idempotency-Key` |
| `DEPENDENCY_UNAVAILABLE` | 503 | Недоступна зависимость или хранилище, открыт circuit breaker, сервис не готов | Повторить после `Retry-After` |

## 7. Идемпотентность команд

**Где обязательна.**

- **Public и admin.** `Idempotency-Key` обязателен для всех POST-команд public и admin, без него → `400 IDEMPOTENCY_KEY_REQUIRED`. Например:
  - `POST /api/v1/trips`, `/trips/{tripId}/finish`;
  - `POST /api/v1/payment-methods`, `/debts/payments`, `/fines/{fineId}/appeals`;
  - `POST /api/v1/photos/upload-sessions`, `/photos/{photoId}/complete`;
  - `POST /api/v1/auth/otp` — повтор возвращает тот же `challengeId` без второй SMS;
  - все admin-POST.
- **Исключение — `POST /api/v1/auth/token` и `/auth/logout`.** Ответ `token` содержит секреты, которые нельзя сохранять в `idempotency_record`, а `logout` идемпотентен сам по себе. Защита — одноразовость OTP и refresh-токена ([user-service.md](services/user-service.md#3-api)).
- **Внутренние команды** могут вместо заголовка опираться на естественный ключ в теле. Такой ключ обязательно указан в документе владельца:

| Эндпоинт | Естественный ключ (и заголовок, если владелец его требует) |
| --- | --- |
| `POST /internal/v1/leases/{leaseId}/commands` | `commandId` |
| `POST /internal/v1/leases/{leaseId}/release` | `leaseId` + `fencingToken`; дополнительно `Idempotency-Key: {tripId}:release` |
| `POST /internal/v1/payment-authorizations` | `operationKey = Idempotency-Key = "{tripId}:authorize"` |
| `POST /internal/v1/payment-authorizations/{authorizationId}/cancel` | `authorizationId`; `Idempotency-Key: {tripId}:cancel` |
| `POST /internal/v1/scooters/{scooterId}/leases` | `tripId` (один lease на поездку) + `Idempotency-Key` |
| `POST /internal/v1/quotes` | `Idempotency-Key: {tripId}:quote` (обязателен; повторная котировка — `{tripId}:quote:2`) |
| `POST /internal/v1/price-calculations` | детерминированный расчёт |
| `POST /internal/v1/geo-checks/finish` | `(tripId, finishOperationId)` |
| `POST /internal/v1/geo-checks/start` | `(tripId, measurementId)` |

  Внутренняя команда без естественного ключа требует `Idempotency-Key`.

**Ключ.**

- Клиент генерирует UUID на одно **намерение** пользователя и использует его во всех повторах этого намерения: по тайм-ауту, после обрыва сети, после перезапуска приложения (ключ сохраняется вместе с незавершённой операцией). Новое намерение — новый ключ.
- **Область действия (`scope`)**: `"<service>:<METHOD> <route-template>:<principal>"`, где `principal` — `user:<sub>`, `operator:<sub>` или `svc:<container-name>`. Пример: `trip:POST /api/v1/trips/{tripId}/finish:user:01937a0e-...`. Одинаковые ключи разных пользователей или эндпоинтов не конфликтуют.
- **Хеш запроса** — SHA-256 от канонического представления: метод + шаблон пути + значения параметров пути и запроса + JSON-тело с отсортированными ключами и без незначащих пробелов.

**Хранение** — таблица `idempotency_record` в БД сервиса, выполняющего команду (общая схема — [хранение данных, 4.3](data-storage.md#43-общие-служебные-таблицы)). Gateway идемпотентностью не занимается. Исключение — сервис без реляционной БД: Discovery хранит записи идемпотентности admin-команд в Redis (ключи `dsc:idem:*`, TTL 24 ч; захват ключа — `SET NX`), алгоритм и коды ответов те же, повтор возвращает тот же `202` ([discovery-service.md](services/discovery-service.md#3-api)).

**Алгоритм.**

1. **Предварительные проверки:** аутентификация, авторизация, лимиты, схема тела. Отказ на этом шаге (`400`/`401`/`403`/`429`) **не создаёт** записи.
2. **Захват ключа** — короткая транзакция, которая фиксируется сразу, чтобы параллельный запрос её увидел:

   ```sql
   INSERT INTO idempotency_record (scope, idempotency_key, request_hash, status, created_at, expires_at)
   VALUES (:scope, :key, :hash, 'IN_PROGRESS', now(), now() + interval '24 hours')
   ON CONFLICT (scope, idempotency_key) DO NOTHING;
   ```

3. **Вставка не прошла** (ключ уже есть) — читаем запись:
   - `request_hash` отличается → `422 IDEMPOTENCY_KEY_REUSED`;
   - `status = 'COMPLETED'` → вернуть сохранённые `response_status` и `response_body` (и `Location`) с `Idempotency-Replayed: true`. Возвращается **исходный** ответ, даже если ресурс с тех пор изменился;
     - **исключение — асинхронные команды, отвечающие `202`** (команды Fleet по `commandId`, `POST /api/v1/photos/{photoId}/complete`). Повтор с тем же ключом может вернуть `202` с **текущим** статусом операции вместо побайтовой копии первого ответа: клиенту нужен актуальный статус, а идентификатор операции и `Location` остаются прежними. Так же Photo отвечает на `complete` с новым ключом для уже загруженного фото. Владелец, выбравший такое поведение, описывает его в документе сервиса;
   - `status = 'IN_PROGRESS'` и запись моложе 60 с → `409 IDEMPOTENCY_IN_PROGRESS`, `Retry-After: 1`;
   - `status = 'IN_PROGRESS'` и запись старше 60 с (исполнитель упал) → перехват: `UPDATE idempotency_record SET created_at = now() WHERE scope = :scope AND idempotency_key = :key AND status = 'IN_PROGRESS' AND created_at = :seenCreatedAt`. Обновлена 1 строка → выполняем команду (шаг 4), 0 строк → ключ перехватил другой исполнитель → `409`.
4. **Исполнение.** Бизнес-изменение, запись в outbox и завершение ключа — в **одной** транзакции:

   ```sql
   UPDATE idempotency_record
      SET status = 'COMPLETED', response_status = :status, response_body = :body, resource_id = :id
    WHERE scope = :scope AND idempotency_key = :key AND status = 'IN_PROGRESS';
   ```

   Падение до коммита означает, что ничего не произошло, и перехват на шаге 3 безопасно выполнит команду заново. Если команда синхронно вызывает внешнюю систему (авторизация у PSP), во внешний вызов передаётся ключ, производный от `(scope, idempotency_key)`. Тогда перехват повторит внешний вызов с тем же ключом, а не создаст второй платёж.
5. **Что сохраняется:**
   - `2xx`, а также детерминированные бизнес-отказы `409`/`422` сохраняются как `COMPLETED`: повтор вернёт тот же отказ, для новой попытки нужен новый ключ;
   - `5xx`, `503` и тайм-ауты не сохраняются: запись удаляется (`DELETE ... WHERE status = 'IN_PROGRESS'`), и клиент повторяет с **тем же** ключом.
6. **Срок жизни** — 24 ч (`expires_at`). Очистка выполняется батчами по `ix_idempotency_expires`. После истечения тот же ключ считается новым запросом.

Идемпотентность не заменяет бизнес-инварианты. Два разных ключа «начать поездку» не создадут две поездки: второй запрос получит `409 ACTIVE_TRIP_EXISTS` за счёт уникального ограничения `user_active_trip`.

## 8. Конкурентный доступ: ETag и If-Match

- Версионируемые ресурсы (профиль, самокат, отчёт о повреждении, тариф и его версии, версия геозон, политика штрафа) отдают `ETag: "<row_version>"`.
- `PATCH` (JSON Merge Patch, RFC 7396) и переходы состояния таких ресурсов (`/publish`, `/validate`, решения по обжалованию) требуют `If-Match`. Нет заголовка → `428 PRECONDITION_REQUIRED`; версия устарела → `412 PRECONDITION_FAILED`, текущий `ETag` возвращается в ответе.
- Реализация — условное обновление:

  ```sql
  UPDATE <table> SET ..., row_version = row_version + 1
   WHERE <id> = :id AND row_version = :ifMatch;
  ```

  0 строк → `412`.
- Неизменяемые ресурсы отдают `ETag` по содержимому и `Cache-Control: immutable`; редко меняющиеся — `ETag` + `If-None-Match` → `304`. Это опубликованные версии тарифов и зон, `GET /api/v1/map/geofences`, `GET /api/v1/tariffs`.
- Внутренние команды вместо ETag используют свои механизмы: fencing token (lease), версия агрегата, естественные ключи.

## 9. Пагинация, фильтры, сортировка

- **Курсорная пагинация.** Запрос `?limit=&cursor=` → `{"items": [...], "nextCursor": "<string>|null"}`. `limit` по умолчанию 20, максимум 100. Массовые выгрузки делаются отдельными асинхронными заданиями, а не большим `limit`.
- **Курсор** непрозрачен для клиента: base64url от `{v, sortKey, filtersHash}` с HMAC-подписью. Используется keyset-пагинация (`WHERE (created_at, id) < (:c, :id)`), поэтому страницы стабильны при вставках и не деградируют на глубоких страницах. Курсор привязан к фильтрам: другие фильтры со старым курсором → `400 INVALID_CURSOR`.
- **Сортировка** по умолчанию — `createdAt DESC, id DESC`; UUIDv7 упорядочен по времени. Параметр `sort` (`sort=-createdAt`) допускается только там, где его описал владелец. Общее количество (`total`) не возвращается: это дорого и на мобильном клиенте не нужно.
- **Фильтры** — query-параметры camelCase; несколько значений перечисляются через запятую (`status=ACTIVE,ENDING`). Интервалы времени полуоткрыты, `from` ≤ t < `to`; максимальное окно задаёт владелец (история перемещений — ≤ 24 ч).

## 10. Лимиты запросов на Gateway

**Алгоритм — token bucket**, счётчики в хранилище Gateway. При недоступности хранилища Gateway переходит на локальные лимиты на инстанс (лимит / число инстансов) и не перестаёт пропускать трафик. Превышение → `429 RATE_LIMITED` + `Retry-After` + `RateLimit`.

**Пулы.** У каждого класса маршрутов свой пул лимитов и свой лимит одновременных запросов к upstream (bulkhead на Gateway), поэтому всплеск поиска не вытесняет старт и завершение поездок.

Значения ниже — оценка, уточняются нагрузочным тестом:

| Маршрут | Ключ | Лимит | Зачем |
| --- | --- | --- | --- |
| `POST /api/v1/auth/otp` | номер телефона (хеш) | 1 / 60 с (= `resendAfterSec`), 5 / ч, 10 / сутки | Стоимость SMS, SMS-pumping |
| `POST /api/v1/auth/otp` | IP | 20 / мин, после превышения — капча | Перебор номеров |
| `POST /api/v1/auth/token` | IP | 30 / мин | Перебор кодов; дополнительно User API: ≤ 5 попыток на `challengeId` |
| `GET /api/v1/scooters/nearby` | пользователь | 5 / с, burst 10 | Панорамирование карты; клиент делает debounce 300 мс |
| `GET /api/v1/scooters/nearby` | IP | 20 / с | Скрейпинг |
| `GET /api/v1/map/geofences` | пользователь | 1 / с, burst 5 | Ответ кешируется по `ETag` |
| `POST /api/v1/trips` | пользователь | 10 / мин | Боты; инвариант «одна активная поездка» действует независимо |
| `POST /api/v1/trips/{tripId}/finish` | пользователь | 10 / мин | Повторные нажатия; повтор с тем же ключом идемпотентен |
| `GET /api/v1/trips/{tripId}/operations/{operationId}` | пользователь | 2 / с | Опрос статуса с интервалом из `Retry-After` |
| `POST /api/v1/photos/upload-sessions` | пользователь | 20 / мин | Защита от всплесков на Gateway, превышение — `429 RATE_LIMITED`. Отдельно действуют ограничения Photo API ([photo-service.md](services/photo-service.md#3-api)): бизнес-квота 20 сессий загрузки в час и 100 в сутки на пользователя → `429 UPLOAD_RATE_LIMITED`; больше 3 активных фото или 6 сессий на пару (пользователь, субъект) → `409 PHOTO_LIMIT_EXCEEDED` |
| `POST /api/v1/payment-methods`, `/debts/payments` | пользователь | 5 / мин | Перебор карт (card testing) |
| Прочие `/api/v1/**` | пользователь | 20 / с | Общий пул |
| `/admin/v1/**` | оператор | 20 / с чтение, 5 / с изменения | Ошибки скриптов и автоматизации |
| `/webhooks/v1/psp/**` | IP провайдера | 200 / с | Защита Billing API при массовой доставке |

При пике ≈ 100 тыс. поездок/ч (A1, условно) это ≈ 30 стартов/с на всю систему; пользовательские лимиты на порядки выше нормального поведения и отсекают только автоматизацию.

Gateway также ограничивает размер тела: JSON — 1 МБ; `POST /admin/v1/geofence-versions` — 20 МБ; бинарные фото через Gateway не идут. Тайм-ауты к upstream: public — 3 с (длительные команды отвечают `202`), admin — 30 с, webhook — 5 с.

## 11. Межсервисные вызовы: тайм-ауты, повторы, circuit breaker

**Стек клиента.** Spring WebClient/RestClient + Resilience4j (TimeLimiter, Retry, CircuitBreaker, Bulkhead), у каждой зависимости свой клиент и свой пул соединений.

- **Тайм-ауты.** Подключение — 100 мс. Ответ — 3–5 × целевой p99 владельца. Вложенные тайм-ауты меньше внешних: вызов внутри обработки запроса должен уложиться в оставшийся бюджет вызывающего.
- **Повторы — только для идемпотентных операций.** Это GET, PUT, DELETE и POST с `Idempotency-Key` или естественным ключом.
  - Не более 2 повторов (3 попытки).
  - Экспоненциальный backoff 50 мс × 2ⁿ с **full jitter**, потолок 1 с на синхронном пути.
  - Повторяются: ошибка подключения, тайм-аут, `502`/`503`/`504`, `429` и `409 IDEMPOTENCY_IN_PROGRESS` (с учётом `Retry-After`, если он укладывается в бюджет).
  - Другие 4xx не повторяются никогда.
- **Бюджет повторов** — не более 10 % запросов клиента к зависимости за скользящие 10 с. Сверх бюджета ошибка возвращается сразу, без повтора: это защита от лавины повторов. Повторяет только один уровень. У шагов durable saga (Trip, Billing) есть второй, медленный уровень — новая попытка шага из БД (`next_run_at`, backoff до минут); быстрые повторы внутри одной попытки подчиняются правилам выше.
- **Circuit breaker:**
  - окно — последние 20 вызовов (клиенты с высоким RPS — окно 10 с по времени);
  - открывается при ≥ 50 % ошибок или ≥ 50 % медленных вызовов (медленный — дольше 0,8 × тайм-аута);
  - в открытом состоянии 10 с (для внешних систем — до 30 с), затем 5 пробных вызовов в half-open.
  - Пока breaker открыт, вызовы сразу завершаются `DEPENDENCY_UNAVAILABLE`, и вызывающий применяет деградацию, описанную в документе сервиса.
- **Bulkhead** — лимит одновременных вызовов на зависимость, чтобы медленная зависимость не занимала все потоки.

Ключевые вызовы (значения — проектные; полная таблица шагов saga — [trip-service.md §3.6](services/trip-service.md#36-исходящие-вызовы-trip-saga-worker)):

| Вызывающий → эндпоинт | Тайм-аут | Повторы | При отказе |
| --- | --- | --- | --- |
| Trip Saga Worker → User `GET /internal/v1/users/{userId}/rental-eligibility` | 500 мс | 1 | Fail-closed: `503`, тайм-аут или открытый CB → старт отменяется, а не продолжается без проверки |
| Trip Saga Worker → Fleet `GET /internal/v1/scooters/{scooterId}` | 500 мс | 2 | Старт не продолжается, повтор шага saga или отказ |
| Trip Saga Worker → Geofence `POST /internal/v1/geo-checks/start`, `/finish` | 400 мс на попытку | 2, с jitter (тот же ключ), затем circuit breaker | Старт запрещён; завершение — резервное правило Trip ([lab-01 §6](lab-01.md#6-нагрузка-масштабирование-и-доступность)) |
| Trip Saga Worker → Pricing `POST /internal/v1/quotes` | 500 мс | 1 (тот же `Idempotency-Key: {tripId}:quote`) | Старт не продолжается |
| Trip Saga Worker → Fleet `POST .../leases`, `.../commands`, `.../release` | 500 мс | 2 (по `tripId`, `commandId`, `{tripId}:release`) | Сверка по `GET /internal/v1/commands/{commandId}?leaseId=` и `GET /internal/v1/leases/{leaseId}` (300 мс) |
| Trip Saga Worker → Billing `POST /internal/v1/payment-authorizations` | 4 с (тайм-аут PSP внутри Billing — 3 с) | 0; повтор — новой попыткой шага с тем же `operationKey` | `PENDING_RECONCILIATION`, без повторного списания; исход Trip узнаёт опросом (следующая строка) — события об этом исходе Billing не публикует |
| Trip Saga Worker → Billing `GET /internal/v1/payment-authorizations/{authorizationId}` | 300 мс | опрос с backoff 1, 2, 4, 8, 16, 30 с | `AUTHORIZED` → разблокировка, `DECLINED` → компенсации; дедлайн 2 мин → `CANCEL_AUTHORIZATION` и `RELEASE_LEASE` ([trip-service.md §3.6](services/trip-service.md#36-исходящие-вызовы-trip-saga-worker)) |
| Trip Saga Worker → Billing `POST /internal/v1/payment-authorizations/{authorizationId}/cancel` | 3 с | backoff ≤ 5 мин, до 24 ч | `CANCEL_PENDING` допустим, Billing досверит с PSP |
| Trip Saga Worker → Geofence `GET /internal/v1/geo-checks/{checkId}` (только разбор `trip_review`) | 300 мс | 2 | `404` или `410 GEO_CHECK_ARCHIVED` → разбор продолжается без итога проверки; старт и завершение этот вызов не используют |
| Trip Saga Worker, Billing Settlement Worker → Pricing `GET /internal/v1/signing-keys` (JWKS подписи котировок) | 300 мс | 1, с jitter | Кеш JWKS 5 мин, неизвестный `kid` → принудительное обновление не чаще раза в 30 с; Pricing недоступен → ключи из кеша. Ключа для `kid` нет: Trip — временный отказ шага, saga повторяет его до дедлайна фазы; Billing — задание расчёта откладывается, алерт |
| Trip Saga Worker → Photo `GET /internal/v1/photos/{photoId}` | 300 мс | 2 | Завершение не блокируется: статус фото приходит событием |
| Billing Settlement Worker → Pricing `POST /internal/v1/price-calculations` | 1 с | 2 | Повтор шага расчёта с backoff |
| Billing API → Photo `GET /internal/v1/photos/{photoId}` | 300 мс | 2 | `503 DEPENDENCY_UNAVAILABLE` на подачу обжалования; клиент повторяет с тем же ключом |
| Billing API → Geofence `GET /internal/v1/violation-evidence/{evidenceId}` | 1 с | 1 | Подача обжалования не блокируется; детали свидетельства для разбора и проверки ручного штрафа читаются позже ([billing-service.md](services/billing-service.md#3-api)); ручной штраф по `violationEvidenceId` при недоступности Geofence → `503` (по `reviewId` — работает); `404 EVIDENCE_NOT_FOUND` по свидетельству назначенного штрафа → решение в пользу пользователя |
| Billing Reconciliation Worker → Trip `GET /internal/v1/trips/{tripId}` | 300 мс | 2 | Открытый CB, тайм-аут или `503` — hold не трогается, проверка «сиротского» hold откладывается: `next_check_at = now() + 5 мин` |
| Fleet API → Telemetry `GET /internal/v1/scooters/{scooterId}/location` (fallback при устаревшей проекции) | 150 мс | 0 | Ответ из проекции Fleet с честным `measurementAgeMs`; выдача lease по устаревшему измерению — `422 MEASUREMENT_STALE` |
| Fleet API → Photo `GET /internal/v1/photos/{photoId}` | 300 мс | 2, затем circuit breaker | Отчёт о повреждении принимается, фото получает статус `PENDING`, итог придёт событием; фото нет (`404`), `EXPIRED` или `DELETED` → `422 PHOTO_NOT_AVAILABLE` |
| Telemetry API → Fleet `GET /internal/v1/scooters/{scooterId}` (`cityId` для проверки `cityIds` оператора) | 300 мс | 1 (кеш 10 мин) | Недоступность и промах кеша → отказ (fail-closed); для `ADMIN` не вызывается |
| Discovery Projector → Fleet `GET /internal/v1/scooters/snapshot` (перестроение) | 2 с | 2, backoff 50 мс × 2ⁿ | Перестроение `FAILED`, старое поколение индекса продолжает работать |
| Discovery Projector → Telemetry `GET /internal/v1/locations/snapshot` (перестроение) | 2 с | 2 | Перестроение `FAILED`, старое поколение работает |
| Discovery Projector → Pricing `GET /internal/v1/tariff-versions/current` (перестроение, досинхронизация тарифов) | 2 с | 2 | Перестроение `FAILED`; досинхронизация повторяется через 30 с, до успеха цены переключаются по уже известным версиям |
| Discovery Projector → Geofence `GET /internal/v1/geofence-versions/current` (перестроение) | 500 мс | 2 | Перестроение `FAILED` |
| Discovery Projector → Geofence `GET /internal/v1/geofence-versions/{versionId}` | 5 с | событие — до восстановления (`pause` партиции, backoff 1 с → 60 с, не DLQ); перестроение — 2 | Карта показывает предыдущую версию зон; алерт, если версия не применена за 30 мин ([discovery-service.md §3.6](services/discovery-service.md#36-исходящие-вызовы-discovery-projector)) |
| Geofence API → Telemetry `GET /internal/v1/signing-keys` | 500 мс | 1 | Ключи из кеша и копии в БД |

## 12. Трассировка и корреляция

- **Трассировка** — W3C Trace Context (`traceparent`, `tracestate`) и OpenTelemetry SDK/агент в каждом контейнере. Для публичных запросов Gateway всегда начинает новую трассу; присланный клиентом `traceparent` сохраняется как span link, потому что внешним данным доверять нельзя.
- **`traceId`** попадает в каждый `problem+json`, в каждую строку JSON-логов (`traceId`, `spanId`, `requestId`, `service`) и в журнал аудита. Персональные данные в логи не пишутся: только идентификаторы, без телефонов и точных координат.
- **Kafka.** Производитель кладёт `traceparent` в колонку `outbox_event.headers`, Outbox CDC переносит её в заголовки Kafka-сообщения. Потребитель открывает span со ссылкой (link) на контекст производителя, а не дочерний span: обработка асинхронная.
- **Корреляция бизнес-потока** (поля конверта события):
  - `correlationId` — идентификатор исходного бизнес-действия (например, `operationId` завершения или `X-Request-Id` admin-команды). Его наследуют все последующие события и шаги;
  - `causationId` — `eventId` события или `X-Request-Id` запроса, которые **непосредственно** вызвали это событие.
- **MQTT 5.** В командах `traceparent` передаётся в user property; устройство возвращает `commandId` в результате.
- **Сэмплирование** — tail-based в OpenTelemetry Collector: сохраняются все трассы с ошибками и медленнее p99 плюс 5 % остальных.

## 13. Маршруты API Gateway

Маршруты заданы точными парами «метод + шаблон пути», а не общим `/api/v1/scooters/**`, чтобы новый эндпоинт сервиса не стал публичным случайно. Неизвестный маршрут → `404`, известный шаблон с другим методом → `405`. Таблица ниже — полный список публикуемых маршрутов, собранный из таблиц «3.1» документов сервисов; новый эндпоинт добавляется сюда в том же изменении, что и в документ владельца. Query-параметры в шаблон не входят.

| Метод и шаблон пути | Upstream (контейнер) | Класс | Аутентификация | Пул лимитов | API |
| --- | --- | --- | --- | --- | --- |
| `POST /api/v1/auth/otp`, `POST /api/v1/auth/token` | User API | public | без JWT (OTP, refresh-токен в теле) | OTP / auth | [user](services/user-service.md#3-api) |
| `POST /api/v1/auth/logout` | User API | public | JWT пользователя | OTP / auth | [user](services/user-service.md#3-api) |
| `GET`, `PATCH`, `DELETE /api/v1/users/me`; `POST /api/v1/users/me/consents` | User API | public | JWT пользователя | общий | [user](services/user-service.md#3-api) |
| `GET /api/v1/scooters/nearby`, `GET /api/v1/scooters/{scooterId}/card`, `GET /api/v1/map/geofences` | Discovery API | public | JWT пользователя | поиск | [discovery](services/discovery-service.md#3-api) |
| `POST /api/v1/scooters/{scooterId}/damage-reports` | Fleet API | public | JWT пользователя | общий | [fleet](services/fleet-service.md#3-api) |
| `POST`, `GET /api/v1/trips`; `GET /api/v1/trips/{tripId}`; `POST /api/v1/trips/{tripId}/finish`; `GET /api/v1/trips/{tripId}/operations/{operationId}` | Trip API | public | JWT пользователя | поездки | [trip](services/trip-service.md#3-api) |
| `GET /api/v1/tariffs` | Pricing API | public | JWT пользователя | общий | [pricing](services/pricing-service.md#3-api) |
| `GET`, `POST /api/v1/payment-methods`; `PATCH`, `DELETE /api/v1/payment-methods/{paymentMethodId}`; `GET /api/v1/payments`, `GET /api/v1/payments/{paymentId}`; `GET /api/v1/debts`, `POST /api/v1/debts/payments`; `GET /api/v1/fines`, `GET /api/v1/fines/{fineId}`, `POST /api/v1/fines/{fineId}/appeals` | Billing API | public | JWT пользователя | платежи | [billing](services/billing-service.md#3-api) |
| `POST /webhooks/v1/psp/{provider}` | Billing API | webhook | подпись PSP + IP allowlist | webhook | [billing](services/billing-service.md#3-api) |
| `POST /api/v1/photos/upload-sessions`, `POST /api/v1/photos/{photoId}/complete`, `GET /api/v1/photos/{photoId}`, `POST /api/v1/photos/{photoId}/view-url` | Photo API | public | JWT пользователя | общий | [photo](services/photo-service.md#3-api) |
| `GET /admin/v1/users`, `GET /admin/v1/users/{userId}`, `POST /admin/v1/users/{userId}/blocks`, `DELETE /admin/v1/users/{userId}/blocks/{blockId}`, `POST /admin/v1/users/{userId}/sessions/revocations` | User API | admin | JWT оператора | admin | [user](services/user-service.md#3-api) |
| `POST`, `GET /admin/v1/scooters`; `GET /admin/v1/scooters/{scooterId}`; `PATCH /admin/v1/scooters/{scooterId}/maintenance`; `POST /admin/v1/scooters/{scooterId}/commands`; `POST /admin/v1/leases/{leaseId}/force-release`; `GET /admin/v1/damage-reports`, `PATCH /admin/v1/damage-reports/{reportId}` | Fleet API | admin | JWT оператора | admin | [fleet](services/fleet-service.md#3-api) |
| `GET /admin/v1/telemetry/scooters/{scooterId}/locations`, `GET /admin/v1/telemetry/access-audit` | Telemetry API | admin | JWT оператора | admin | [telemetry](services/telemetry-service.md#3-api) |
| `POST /admin/v1/discovery/rebuilds`, `GET /admin/v1/discovery/rebuilds/{rebuildId}` | Discovery API | admin | JWT оператора | admin | [discovery](services/discovery-service.md#3-api) |
| `GET /admin/v1/trips`, `GET /admin/v1/trips/{tripId}`, `POST /admin/v1/trips/{tripId}/force-finish`, `POST /admin/v1/trips/{tripId}/reconciliation-decisions`, `POST /admin/v1/trips/{tripId}/saga-steps/{stepId}/retry`; `GET /admin/v1/trip-reviews`, `POST /admin/v1/trip-reviews/{reviewId}/decision` | Trip API | admin | JWT оператора | admin | [trip](services/trip-service.md#3-api) |
| `POST`, `GET /admin/v1/tariffs`; `POST`, `GET /admin/v1/tariffs/{tariffId}/versions`; `PATCH /admin/v1/tariffs/{tariffId}/versions/{version}`; `POST /admin/v1/tariffs/{tariffId}/versions/{version}/publish`; `GET /admin/v1/price-calculations` | Pricing API | admin | JWT оператора | admin | [pricing](services/pricing-service.md#3-api) |
| `GET /admin/v1/payments`, `POST /admin/v1/payments/{paymentId}/refunds`; `GET`, `POST /admin/v1/fines`; `POST /admin/v1/fines/{fineId}/appeals/{appealId}/decision`; `GET`, `POST /admin/v1/fine-policies/{policyCode}/versions` | Billing API | admin | JWT оператора | admin | [billing](services/billing-service.md#3-api) |
| `GET /admin/v1/geofences`; `GET`, `POST /admin/v1/geofence-versions`; `GET /admin/v1/geofence-versions/{versionId}`; `POST /admin/v1/geofence-versions/{versionId}/validate`, `POST /admin/v1/geofence-versions/{versionId}/publish`; `GET /admin/v1/violation-evidence/{evidenceId}`; `GET /admin/v1/geo-checks` | Geofence API | admin | JWT оператора | admin | [geofence](services/geofence-service.md#3-api) |
| `GET /admin/v1/photos`; `POST /admin/v1/photos/{photoId}/view-url`; `POST /admin/v1/photos/{photoId}/holds`, `DELETE /admin/v1/photos/{photoId}/holds/{holdId}`; `DELETE /admin/v1/photos/{photoId}`; `POST /admin/v1/photos/{photoId}/reprocess` | Photo API | admin | JWT оператора | admin | [photo](services/photo-service.md#3-api) |
| `/internal/**`, `/.well-known/**`, `/actuator/**` | — | — | не публикуются | — | — |

Gateway отвечает за:

- TLS 1.3 и HSTS;
- проверку JWT и ролей по области пути;
- лимиты и bulkhead на upstream;
- лимит размера тела;
- CORS — только origin операторской консоли для `/admin`;
- нормализацию заголовков и `X-Request-Id`;
- access-логи.

Бизнес-авторизацию, агрегацию ответов и преобразование тел Gateway не выполняет. Бинарные фото загружаются по presigned URL напрямую в объектное хранилище, минуя Gateway.

## 14. MQTT

**Брокер** — EMQX-кластер, MQTT 5 поверх TLS 1.3 с mTLS (порт 8883).

- **Сертификат устройства.** Выдаётся при производстве или регистрации устройства, `CN = scooterId`, срок 1 год. Отзыв — через CRL/OCSP; скомпрометированный самокат отключается.
- **Платформенные клиенты** (Telemetry Ingestor, Fleet Command Worker) подключаются с сервисными сертификатами.

**ACL.** Устройство публикует только в `devices/{CN}/telemetry`, `devices/{CN}/command-results` и `devices/{CN}/state`, а подписывается только на `devices/{CN}/commands`. Платформенным клиентам разрешены их shared subscriptions и публикация в `devices/+/commands` (только Fleet Command Worker).

| Топик | Направление | Публикует | Подписчик | QoS | Содержимое (ключевые поля) |
| --- | --- | --- | --- | --- | --- |
| `devices/{scooterId}/telemetry` | устройство → платформа | Бортовая система | Telemetry Ingestor, `$share/telemetry-ingestor/devices/+/telemetry` | 1 | `schemaVersion`, `bootId`, `seq`, `lat`, `lon`, `accuracyM`, `speedKmh`, `batteryPercent`, `lockState`, `measuredAt`. `measurementId`, `receivedAt` и подпись добавляет Ingestor |
| `devices/{scooterId}/commands` | платформа → устройство | Fleet Command Worker | Бортовая система | 1 | `schemaVersion`, `commandId`, `type: UNLOCK\|LOCK\|QUERY_STATE`, `fencingToken`, `leaseId`, `issuedAt`, `expiresAt` |
| `devices/{scooterId}/command-results` | устройство → платформа | Бортовая система | Fleet Command Worker, `$share/fleet-command-worker/devices/+/command-results` | 1 | `schemaVersion`, `commandId`, `status: RECEIVED\|EXECUTED\|FAILED\|REJECTED_STALE\|EXPIRED`, `lockState`, `fencingToken`, `deviceTime`, `failureReason` |
| `devices/{scooterId}/state` | устройство → платформа | Бортовая система (`kind: STATE` — ответ на `QUERY_STATE`; `kind: BIRTH` — после каждого подключения), брокер (`kind: LWT`) | Fleet Command Worker, `$share/fleet-command-worker/devices/+/state` | 1 | `schemaVersion`, `kind: STATE\|BIRTH\|LWT`, `online`, `lockState`, `lastFencingToken`, `recentCommands`, `deviceTime`, `connSeq` (LWT — только `online: false` и `connSeq`) |

Полные форматы сообщений описаны в [telemetry-service.md §3.5](services/telemetry-service.md#35-mqtt) (телеметрия) и [fleet-service.md](services/fleet-service.md#3-api) (команды, результаты, состояние). Общие правила:

- **Формат** — JSON UTF-8 с полем `schemaVersion`, до 4 КБ на сообщение; время — RFC 3339 UTC. Буфер, накопленный без связи, устройство отправляет по порядку `seq`.
- **Сессия:**
  - `cleanStart = false`, `sessionExpiryInterval = 1 ч`: команды, отправленные при кратком обрыве связи, будут доставлены;
  - keepalive 30 с;
  - LWT `devices/{id}/state` с `kind: LWT`, `online: false` и `connSeq`; после каждого подключения устройство публикует `BIRTH`. Сигналы со старым `connSeq` Fleet игнорирует ([fleet-service.md §3.5](services/fleet-service.md#35-mqtt)).
- **Срок жизни команды.** Команда публикуется с `messageExpiryInterval = expiresAt − now`, поэтому устаревший `UNLOCK` не будет доставлен после переподключения. Устройство дополнительно сверяет `expiresAt` со своими часами.
- **Подтверждения.** PUBACK в MQTT подтверждает только участок до брокера:
  - **Команды.** Брокер принял команду (PUBACK для Fleet Command Worker) — это статус `SENT`, но не исполнение. `DELIVERED`, `EXECUTED` и `FAILED` выставляются только по сообщениям устройства в `command-results`. Нет результата до `expiresAt` → `EXPIRED` или `UNKNOWN` и сверка через `QUERY_STATE`. Машина состояний описана в Fleet.
  - **Входящие сообщения.** Устройство хранит сообщение в буфере до PUBACK брокера. Ingestor и Fleet Command Worker подтверждают сообщение брокеру после надёжной записи (пакета) в свою БД. При падении до подтверждения брокер доставит сообщение повторно.
- **Дедупликация:** телеметрия — по `(scooterId, bootId, seq)`: `bootId` — счётчик загрузок устройства, без него `seq` неоднозначен после перезагрузки. Из этой тройки Ingestor вычисляет детерминированный `measurementId = UUIDv5(NS_TLM, "{scooterId}|{bootId}|{seq}")` — единственное определение в [telemetry-service.md §3.3.1](services/telemetry-service.md#331-каноническая-строка-и-подпись). Измерение с `measuredAt` > `receivedAt` + 2 с (тот же допуск, что у `CLOCK_SKEW` в Geofence) Ingestor отклоняет: не пишет в историю и не публикует, только метрика ([telemetry-service.md §2.1.2](services/telemetry-service.md#212-валидация-и-флаги-качества)). Результаты команд — по `commandId`. Устройство помнит последние `commandId` и не исполняет команду повторно, но повторно отправляет её результат.
- **Ограничения брокера на устройство:** не более 10 неподтверждённых сообщений (inflight), не более 2 публикаций/с. Неисправное или взломанное устройство не перегрузит брокер.

## 15. События (Kafka)

### 15.1. Конверт

Значение Kafka-сообщения — конверт события в JSON. Ключ сообщения — из [каталога](#157-каталог-топиков). В заголовках передаются `traceparent`, `eventType`, `schemaVersion` и `content-type: application/json`.

```json
{
  "eventId": "01a1ab15-8a00-7c01-9d02-1e2f3a4b5c6d",
  "eventType": "ScooterAvailabilityChanged",
  "schemaVersion": 1,
  "aggregateType": "Scooter",
  "aggregateId": "01937a10-5c3e-7a11-8f20-1b2c3d4e5f60",
  "aggregateVersion": 1842,
  "occurredAt": "2026-11-05T09:41:13.207Z",
  "producer": "fleet-command-worker",
  "correlationId": "01a1ab15-7700-7c10-8d20-3e4f5a6b7c8d",
  "causationId": "01a1ab15-8900-7b11-8c12-0d1e2f3a4b5c",
  "payload": {
    "scooterId": "01937a10-5c3e-7a11-8f20-1b2c3d4e5f60",
    "cityId": "01900000-0000-7000-8000-000000000001",
    "availability": "AVAILABLE",
    "batteryPercent": 64,
    "model": "NB-G30-2025",
    "changedAt": "2026-11-05T09:41:13.180Z"
  }
}
```

| Поле | Тип | Правило |
| --- | --- | --- |
| `eventId` | UUIDv7 (у `LocationObserved` — UUIDv5) | Уникален глобально; ключ дедупликации в inbox. Для `LocationObserved` `eventId = measurementId` (детерминированный UUIDv5, [telemetry-service.md §3.3.1](services/telemetry-service.md#331-каноническая-строка-и-подпись)), поэтому повторная публикация даёт тот же `eventId` |
| `eventType` | string | PascalCase, прошедшее время; у каждого типа один владелец |
| `schemaVersion` | int | Версия схемы payload внутри major топика (растёт при аддитивных изменениях) |
| `aggregateType`, `aggregateId` | string | Агрегат-источник факта. Ключ Kafka может отличаться: для `PaymentStatusChanged` агрегат — платёж, а ключ — `userId` ради порядка по пользователю |
| `aggregateVersion` | int64 | Монотонна в пределах агрегата; потребитель отбрасывает версию ≤ применённой |
| `occurredAt` | RFC 3339 | Бизнес-время факта, а не время публикации |
| `producer` | string | Kebab-имя контейнера ([15.6](#156-именование)) |
| `correlationId`, `causationId` | UUIDv7 | [Раздел 12](#12-трассировка-и-корреляция) |
| `payload` | object | Бизнес-поля ([15.8](#158-обязательные-поля-payload)) |

**Публикация через outbox:**

- В `outbox_event.payload` пишется **конверт целиком**. Колонки `topic`, `message_key`, `event_type`, `aggregate_id` и `headers` нужны для маршрутизации и заголовков.
- Outbox CDC (Debezium Outbox Event Router) читает WAL и публикует сообщение в `topic` с ключом `message_key`.
- Все производители одного топика пишут в одну БД сервиса, поэтому порядок по ключу сохраняется: CDC читает WAL в порядке коммитов.
- Исключение — `LocationObserved`: Telemetry Ingestor публикует его напрямую после коммита пакета.

**Настройки Kafka:** RF = 3, `min.insync.replicas = 2`, производитель `acks = all`, `enable.idempotence = true`, сжатие `zstd`, `unclean.leader.election.enable = false`. Размер сообщения — до 1 МБ, у `geofence.version.v1` — до 4 МБ из-за `zonesSummary`.

### 15.2. Schema Registry и эволюция схем

- **Схема** payload — JSON Schema (2020-12) в Schema Registry, subject `<topic>-<eventType>` (TopicRecordNameStrategy): в одном топике может быть несколько типов событий. Совместимость — **BACKWARD**.
- **Регистрация.** Схемы регистрируются конвейером CI после проверки совместимости; в production автоматическая регистрация выключена (`auto.register.schemas = false`). Сериализатор производителя валидирует payload по схеме.
- **Открытая модель** (`additionalProperties: true`). Производитель может добавить необязательные поля ([раздел 4](#4-версионирование)), потребитель неизвестные поля игнорирует.
- **Без нового топика допускается:**
  - новое необязательное поле;
  - новое значение enum в поле, помеченном расширяемым (потребитель обрабатывает неизвестное значение как «прочее» или направляет на ручной разбор, но не падает);
  - ослабление ограничений;
  - новый тип события в топике (потребитель пропускает неизвестный `eventType` и фиксирует offset).
- **Ломающее изменение означает новый топик `<...>.v2`:**
  - удаление или переименование поля;
  - смена типа, единиц или смысла;
  - перевод поля в обязательные;
  - смена ключа сообщения.
- **Миграция на `v2`.** Производитель ≥ 30 дней публикует `v1` и `v2` одновременно (две записи outbox в одной транзакции). Потребители переходят на `v2`, после этого `v1` выводится из эксплуатации. Для compact-топиков `v2` перед переключением заполняется публикацией текущего состояния всех агрегатов.
- **Число партиций** увеличивается только с планом: смена числа партиций меняет распределение ключей и временно нарушает порядок.

### 15.3. Ключи, партиции и порядок

- Порядок гарантирован **только внутри ключа** (партиции). Порядка между ключами и между топиками нет; потребитель не должен его предполагать.
- Ключ — идентификатор агрегата или сущности, по которой потребителю нужен порядок (см. каталог). Число партиций выбрано по требуемому параллелизму потребителей.
- Защита от устаревших и повторных событий — `aggregateVersion`. Проекция хранит применённую версию агрегата и обновляется условно (`... WHERE stored_version < :aggregateVersion`).

### 15.4. Доставка: at-least-once и inbox

- **Гарантия доставки — at-least-once.** Дубли возможны: повтор CDC после рестарта, повтор производителя, rebalance потребителя. Сквозного exactly-once с БД, MQTT и PSP нет.
- **Потребитель с БД** работает с `enable.auto.commit = false`. Для каждого сообщения в одной транзакции:
  1. `INSERT INTO inbox_message (consumer_group, event_id) ... ON CONFLICT DO NOTHING`. Если вставлено 0 строк, это дубль: пропустить.
  2. Применить бизнес-эффект (и при необходимости записать outbox).
  3. `COMMIT`.

  Offset фиксируется **после** коммита БД. Падение между коммитом БД и коммитом offset вызывает повторную доставку, которую отсечёт inbox.
- **Схема `inbox_message`** (и `outbox_event`) общая для всех сервисов — [хранение данных, 4.3](data-storage.md#43-общие-служебные-таблицы).
- **Проекции без транзакционной БД** (Discovery Index, кеш Geofence) идемпотентны по `aggregateVersion` вместо inbox. Без inbox работает и Fleet Event Worker на `LocationObserved`: проекция последнего местоположения обновляется условным UPSERT по `(measured_at, seq)`, дубль или устаревшее измерение ничего не меняют. Кеш Geofence API к тому же читает топик без consumer group и без фиксации offset (`assign()`) — документированное исключение из [15.6](#156-именование).
- **Срок дедупликации.** Inbox хранится 14 дней. Повторы старше этого срока возможны только при ручном replay или сбросе offset; от них защищают бизнес-ключи (уникальные ограничения на `tripId`, `fineId`, `paymentId`).
- **Внутри партиции** обработка последовательная; параллелизм даёт число партиций.

### 15.5. Ошибки обработки, DLQ, replay

- **Временная ошибка** — недоступность БД или другого сервиса, тайм-аут, открытый circuit breaker, failover. Повтор на месте с backoff 1 с → 60 с с jitter; партиция приостанавливается (`pause`), сообщение не пропускается, иначе нарушится порядок. В DLQ временные ошибки **никогда** не уходят. Например, Discovery Projector при недоступном Geofence во время обработки `GeofenceVersionPublished` ждёт восстановления, а не отправляет событие в DLQ. Алерт срабатывает по возрасту самого старого необработанного сообщения или лагу группы.
- **Постоянная ошибка** — не проходит схема, ошибка сериализации или десериализации, нарушен инвариант, ссылка на несуществующий агрегат после истечения окна ожидания. Сообщение уходит в DLQ, обработка продолжается со следующего.
  - DLQ: `<topic>.dlq.<consumer-group>`, тот же ключ, ретенция 14 дней.
  - Заголовки: `dlq.reason`, `dlq.exception`, `dlq.original.topic`, `dlq.original.partition`, `dlq.original.offset`, `dlq.consumer`, `dlq.attempts`, `dlq.failedAt`.
  - Потребитель, чувствительный к порядку (шаги saga в Trip), «паркует» ключ: последующие события того же ключа тоже уходят в DLQ, пока первое не разобрано.
- **Replay:**
  - Потребитель содержит второй listener на свой DLQ — тот же обработчик и тот же inbox. Он выключен по умолчанию и включается оператором платформы для диапазона offset или списка `eventId` после исправления причины.
  - Успешно обработанное сообщение фиксируется в DLQ-группе; снова упавшее остаётся в DLQ с алертом.
  - В исходный топик сообщения **не** переотправляются: остальные потребители получили бы лишние дубли.
- **Перестроение проекции целиком:** сброс offset группы на время или snapshot-эндпоинты владельца (`/internal/v1/scooters/snapshot`, `/internal/v1/locations/snapshot`).
- **Мониторинг:** лаг каждой группы, поступление сообщений в DLQ (> 0 → алерт), доля дублей в inbox, ошибки сериализации.

### 15.6. Именование

- **Топик**: `<context>.<stream>.v<major>`, нижний регистр, kebab-case внутри сегмента: `fleet.scooter-availability.v1`. `<context>` — сервис-владелец.
- **Событие**: PascalCase, прошедшее время (`TripEnded`, `GeofenceVersionPublished`).
- **Consumer group** = kebab-имя контейнера потребителя. Это же имя — сервисная идентичность mTLS, значение `producer` в конверте и элемент allowlist:

| Контейнер | Имя | Контейнер | Имя |
| --- | --- | --- | --- |
| User API | `user-api` | Trip API | `trip-api` |
| User Worker | `user-worker` | Trip Saga Worker | `trip-saga-worker` |
| Fleet API | `fleet-api` | Pricing API | `pricing-api` |
| Fleet Command Worker | `fleet-command-worker` | Billing API | `billing-api` |
| Fleet Event Worker | `fleet-event-worker` | Billing Settlement Worker | `billing-settlement-worker` |
| Telemetry Ingestor | `telemetry-ingestor` | Billing Reconciliation Worker | `billing-reconciliation-worker` |
| Telemetry API | `telemetry-api` | Geofence API | `geofence-api` |
| Discovery API | `discovery-api` | Photo API | `photo-api` |
| Discovery Projector | `discovery-projector` | Photo Processing Worker | `photo-processing-worker` |

- **Исключение — кеш Geofence API.** Он читает `geofence.version.v1` **без consumer group**: каждая реплика вызывает `assign()` на все партиции топика (не `subscribe()`) и не фиксирует offset, потому что событие должна получить каждая реплика. Состояние восстанавливается из Geofence DB, DLQ нет ([geofence-service.md](services/geofence-service.md#34-потребляемые-события)).
- **Служебное чтение без группы — Telemetry `KafkaBackfillLoader`.** После отказа Telemetry DB он читает собственный `telemetry.location.v1` через `assign()` с `offsetsForTimes`, offset не фиксирует и consumer group не создаёт ([telemetry-service.md](services/telemetry-service.md#34-потребляемые-события)). Других потребителей без группы нет.
- **DLQ**: `<topic>.dlq.<consumer-group>`, например `trip.lifecycle.v1.dlq.billing-settlement-worker`.
- **Subject схемы**: `<topic>-<eventType>`, например `billing.fine.v1-FineIssued`.

### 15.7. Каталог топиков

Все топики — RF = 3, `min.insync.replicas = 2`. Для compact-топиков `min.compaction.lag.ms = 1 ч`. Поля payload — в [15.8](#158-обязательные-поля-payload).

| Топик | События | Ключ | Партиций / ретенция | Производитель (контейнер) | Потребители → consumer group | DLQ |
| --- | --- | --- | --- | --- | --- | --- |
| `user.access.v1` | `UserAccessChanged` | `userId` | 24 / 7 д | User API, User Worker (outbox) | Trip Saga Worker → `trip-saga-worker`; Photo Processing Worker → `photo-processing-worker`; Billing Settlement Worker → `billing-settlement-worker` (`DELETED` → деактивация способов оплаты и удаление токенов у PSP; финансовые записи хранятся 5 лет псевдонимизированными) | `user.access.v1.dlq.trip-saga-worker`, `user.access.v1.dlq.photo-processing-worker`, `user.access.v1.dlq.billing-settlement-worker` |
| `fleet.scooter-availability.v1` | `ScooterAvailabilityChanged` | `scooterId` | 48 / compact | Fleet API, Fleet Command Worker, Fleet Event Worker (outbox) | Discovery Projector → `discovery-projector` | `fleet.scooter-availability.v1.dlq.discovery-projector` |
| `fleet.device-command.v1` | `DeviceCommandUpdated` | `scooterId` | 48 / 7 д | Fleet Command Worker (outbox). Только команды с `leaseId` (команды поездки) и только статусы `SENT`, `DELIVERED`, `EXECUTED`, `FAILED`, `EXPIRED`, `UNKNOWN`; `ACCEPTED` и сервисные команды оператора (`leaseId = null`) не публикуются — они видны через admin API и аудит | Trip Saga Worker → `trip-saga-worker` | `fleet.device-command.v1.dlq.trip-saga-worker` |
| `telemetry.location.v1` | `LocationObserved` (подписанное) | `scooterId` | 96 / 24 ч | Telemetry Ingestor (напрямую после коммита, без outbox; повтор безопасен — `measurementId`) | Discovery Projector → `discovery-projector`; Fleet Event Worker → `fleet-event-worker` | `telemetry.location.v1.dlq.discovery-projector`, `telemetry.location.v1.dlq.fleet-event-worker` |
| `pricing.tariff-version.v1` | `TariffVersionPublished` | `tariffId` | 6 / compact | Pricing API (outbox) | Discovery Projector → `discovery-projector` | `pricing.tariff-version.v1.dlq.discovery-projector` |
| `geofence.version.v1` | `GeofenceVersionPublished` | `cityId` | 6 / compact; `max.message.bytes = 4 МБ` (из-за `zonesSummary`), сжатие `zstd` | Geofence API (outbox) | Discovery Projector → `discovery-projector`; Geofence API — обновление кеша реплик, **без группы**: `assign()` всех партиций, offset не фиксируется ([15.6](#156-именование)) | `geofence.version.v1.dlq.discovery-projector`; у кеша Geofence API DLQ нет |
| `trip.lifecycle.v1` | `TripEnded` | `tripId` | 24 / 14 д | Trip Saga Worker (outbox) | Billing Settlement Worker → `billing-settlement-worker` | `trip.lifecycle.v1.dlq.billing-settlement-worker` |
| `billing.payment-status.v1` | `PaymentStatusChanged` | `userId` | 24 / 14 д | Billing API (погашение долга), Billing Settlement Worker (расчёт, повторы, возвраты), Billing Reconciliation Worker (исходы из webhook и сверки) (outbox). Исход авторизации `PENDING_RECONCILIATION` событием не публикуется: Trip опрашивает `GET /internal/v1/payment-authorizations/{authorizationId}` | Trip Saga Worker → `trip-saga-worker`; User Worker → `user-worker` | `billing.payment-status.v1.dlq.trip-saga-worker`, `billing.payment-status.v1.dlq.user-worker` |
| `billing.fine.v1` | `FineIssued`, `FineStatusChanged` | `userId` | 12 / 30 д | Billing Settlement Worker (`FineIssued` с `source = AUTO`; `FineStatusChanged` при списании), Billing API (`FineIssued` с `source = MANUAL`, обжалование и решения по нему — `FineStatusChanged`), Billing Reconciliation Worker (`FineStatusChanged(PAID)` по исходу сверки с PSP) (outbox) | Trip Saga Worker → `trip-saga-worker`; User Worker → `user-worker`; Photo Processing Worker → `photo-processing-worker` | `billing.fine.v1.dlq.trip-saga-worker`, `billing.fine.v1.dlq.user-worker`, `billing.fine.v1.dlq.photo-processing-worker` |
| `photo.status.v1` | `PhotoValidated`, `PhotoRejected` | `photoId` | 24 / 7 д | Photo Processing Worker (outbox). Истечение (`EXPIRED`) и удаление (`DELETED`) фото событий не порождают: актуальный статус потребитель читает `GET /internal/v1/photos/{photoId}` | Trip Saga Worker → `trip-saga-worker`; Fleet Event Worker → `fleet-event-worker` | `photo.status.v1.dlq.trip-saga-worker`, `photo.status.v1.dlq.fleet-event-worker` |

### 15.8. Обязательные поля payload

Производитель может добавлять поля, но не переименовывать и не удалять перечисленные. `|null` — поле присутствует со значением `null`. Детали и примеры в конверте — в разделе «3.3. Публикуемые события» документа владельца.

| Событие | Владелец | Поля payload |
| --- | --- | --- |
| `UserAccessChanged` | [User](services/user-service.md#3-api) | `userId`, `accessStatus: ACTIVE\|RESTRICTED\|BLOCKED\|DELETED`, `reasons[]: DEBT\|UNPAID_FINE\|ADMIN_BLOCK\|VERIFICATION_REQUIRED`, `canStartTrip`, `effectiveAt`; добавлено `detailReasons[]` — полный внутренний список причин (например, заявка на удаление: `RESTRICTED` + `detailReasons: [ACCOUNT_DELETION_PENDING]`, enum `reasons` не расширяется); `accessVersion` = `aggregateVersion` конверта |
| `ScooterAvailabilityChanged` | [Fleet](services/fleet-service.md#3-api) | `scooterId`, `cityId`, `availability: AVAILABLE\|RESERVED\|IN_USE\|MAINTENANCE\|OFFLINE\|LOW_BATTERY\|RECONCILING`, `batteryPercent`, `model`, `changedAt`; добавлено: `operationalStatus`, `previousAvailability`, `reason`; `aggregateVersion = scooter.version` |
| `DeviceCommandUpdated` | [Fleet](services/fleet-service.md#3-api) | `commandId`, `scooterId`, `leaseId`, `tripId\|null`, `commandType: UNLOCK\|LOCK\|QUERY_STATE`, `status: SENT\|DELIVERED\|EXECUTED\|FAILED\|EXPIRED\|UNKNOWN` (`ACCEPTED` не публикуется — он виден только в `202` и `GET` команды; команды без `leaseId` не публикуются), `result {lockState: OPEN\|CLOSED\|UNKNOWN, deviceTime, serverReceivedAt}\|null`, `failureReason\|null` |
| `LocationObserved` | [Telemetry](services/telemetry-service.md#3-api) | `measurementId` (UUIDv5 от `{scooterId}\|{bootId}\|{seq}`, не UUIDv7), `scooterId`, `bootId`, `seq`, `lat`, `lon`, `accuracyM`, `speedKmh`, `batteryPercent`, `lockState`, `measuredAt`, `receivedAt`, `qualityFlags[]: LOW_ACCURACY\|IMPLAUSIBLE_JUMP\|LATE` (`LATE` — только в режиме `KAFKA_ONLY`), `signature {alg, kid, value}`. Каноническая подписываемая строка (`TLM1`, точность координат и времени) — единственное определение в [telemetry-service.md §3.3.1](services/telemetry-service.md#331-каноническая-строка-и-подпись); потребители на него ссылаются и не пересказывают |
| `TariffVersionPublished` | [Pricing](services/pricing-service.md#33-публикуемые-события) | `tariffId`, `cityId`, `version`, `effectiveFrom`, `currency`, `unlockFeeMinor`, `perMinuteMinor`, `roundingRule`, `displayName`; добавлено: `applicableModels\|null` (`null` — все модели, `[]` — тариф снят с продажи), `priority`, `minimumChargeMinor`, `maxTripChargeMinor`, `algorithmVersion`, `timeZone`. Топик compact по `tariffId`, будущая версия вытесняет текущую: полное состояние (текущая + следующая, у `next` тот же набор полей версии) — `GET /internal/v1/tariff-versions/current` |
| `GeofenceVersionPublished` | [Geofence](services/geofence-service.md#33-публикуемые-события) | `cityId`, `versionId`, `version`, `effectiveFrom`, `checksum`, `zonesSummary[] {zoneId, type: OPERATING_AREA\|PARKING\|NO_PARKING\|NO_RIDE\|SLOW_ZONE, speedLimitKmh\|null}`; геометрия — `GET /internal/v1/geofence-versions/{versionId}` |
| `TripEnded` | [Trip](services/trip-service.md#3-api) | `tripId`, `userId`, `scooterId`, `cityId`, `startedAt`, `endedAt`, `finishRequestedAt`, `durationSec`, `tariffSnapshot {quoteId, tariffId, tariffVersion, algorithmVersion, snapshotHash, signature, snapshot}`, `authorizationId`, `startCheckId`, `finishCheck {checkId\|null, decision: ALLOWED\|ALLOWED_WITH_VIOLATION\|FORBIDDEN\|NOT_CHECKED, reasonCode\|null, policyVersion\|null (int32, монотонна в пределах `cityId`), violationEvidenceId\|null}`, `parkingPhoto {photoId\|null, status: PENDING\|VALIDATED\|REJECTED\|MISSING, flags[], reason\|null}`, `endReason: USER_FINISH\|SUPPORT_FORCE_FINISH`. `cityId` — UUID. `FORBIDDEN` и `NOT_CHECKED` — только при `SUPPORT_FORCE_FINISH`; `NOT_CHECKED` (без геопроверки) — все поля `finishCheck`, кроме `decision`, равны `null`, автоматический штраф не назначается. `reasonCode` (словарь Geofence): `OK`, `BOUNDARY_TOLERANCE`, `OUTSIDE_OPERATING_AREA`, `NO_RIDE_ZONE`, `NO_PARKING_ZONE`, `PARKING_OUTSIDE_ZONE`, `MEASUREMENT_STALE`, `LOW_ACCURACY`, `CLOCK_SKEW`. `parkingPhoto.reason` — `PhotoRejected.reason` или `OWNERSHIP_MISMATCH`. Потребители терпят неизвестные значения enum |
| `PaymentStatusChanged` | [Billing](services/billing-service.md#3-api) | `paymentId`, `userId`, `tripId\|null`, `fineId\|null`, `kind: TRIP_CHARGE\|FINE\|DEBT_REPAYMENT\|REFUND`, `status: PENDING\|CAPTURED\|RETRYING\|DEBT\|PAID\|REFUNDED\|FAILED`, `amount`, `outstandingDebtMinor`, `changedAt`; добавлено: `paidAmount\|null`, `declineCode\|null`, `nextRetryAt\|null`, `debtVersion` (int64, монотонна в пределах `userId`, растёт в той же транзакции, что и изменение долга; User применяет `outstandingDebtMinor`, только если `debtVersion` больше сохранённой) |
| `FineIssued` | [Billing](services/billing-service.md#3-api) | `fineId`, `userId`, `tripId`, `violationEvidenceId` (`\|null` только у `MANUAL` с `reviewId`), `finePolicyCode`, `finePolicyVersion`, `amount`, `reasonCode`, `photoIds[]`, `issuedAt`; добавлено: `appealDeadline`, `source: AUTO\|MANUAL` (расширяемый), `reviewId\|null` |
| `FineStatusChanged` | [Billing](services/billing-service.md#3-api) | `fineId`, `userId`, `tripId`, `status: APPEALED\|UPHELD\|CANCELLED\|PAID`, `photoIds[]`, `changedAt`; добавлено: `appealId\|null` |
| `PhotoValidated` | [Photo](services/photo-service.md#3-api) | `photoId`, `ownerUserId`, `purpose`, `subjectType`, `subjectId`, `sha256`, `capturedAt\|null`, `flags[]: SUSPECTED_REUSE\|LOW_QUALITY\|NO_SCOOTER_DETECTED\|EXIF_TIME_MISMATCH`, `validatedAt` |
| `PhotoRejected` | [Photo](services/photo-service.md#3-api) | `photoId`, `ownerUserId`, `purpose`, `subjectType`, `subjectId`, `reason: SIZE_MISMATCH\|CHECKSUM_MISMATCH\|UNSUPPORTED_FORMAT\|CORRUPTED\|TOO_SMALL\|DUPLICATE`, `rejectedAt` |

## 16. Документы сервисов

| Сервис | API (эндпоинты, события, MQTT) |
| --- | --- |
| User | [services/user-service.md#3-api](services/user-service.md#3-api) |
| Fleet | [services/fleet-service.md#3-api](services/fleet-service.md#3-api) |
| Telemetry | [services/telemetry-service.md#3-api](services/telemetry-service.md#3-api) |
| Discovery | [services/discovery-service.md#3-api](services/discovery-service.md#3-api) |
| Trip | [services/trip-service.md#3-api](services/trip-service.md#3-api) |
| Pricing | [services/pricing-service.md#3-api](services/pricing-service.md#3-api) |
| Billing | [services/billing-service.md#3-api](services/billing-service.md#3-api) |
| Geofence | [services/geofence-service.md#3-api](services/geofence-service.md#3-api) |
| Photo | [services/photo-service.md#3-api](services/photo-service.md#3-api) |
