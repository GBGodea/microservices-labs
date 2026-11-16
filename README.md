# Лабораторная работа №1 — декомпозиция системы на микросервисы

**Предметная область:** краткосрочная аренда электросамокатов.  
**Вариант:** 2224. **Масштаб:** 5 млн пользователей, 5 000 RPS.

| Артефакт | Файл |
| --- | --- |
| Анализ области, bounded contexts, взаимодействия, нагрузка и риски | [Отчёт](docs/lab-01.md) |
| Каталог девяти микросервисов (сводка) | [service-catalog.md](docs/service-catalog.md) |
| Детальное описание каждого сервиса: C2, C3 каждого контейнера, API, данные, масштабирование | [docs/services/](docs/services/) |
| Общие соглашения API, каталог топиков Kafka и MQTT | [api-conventions.md](docs/api-conventions.md) |
| Хранение данных: почему PostgreSQL, индексы, партиционирование, шардирование, HA | [data-storage.md](docs/data-storage.md) |
| ADR-001: декомпозиция, альтернативы и последствия | [ADR-001](docs/adr/ADR-001-decomposition.md) |
| ADR-002: выбор хранилищ | [ADR-002](docs/adr/ADR-002-storage.md) |
| ADR-003: фотофиксация и хранение фото | [ADR-003](docs/adr/ADR-003-photo-evidence.md) |
| Context Map | [SVG](diagrams/rendered/context-map.svg) · [исходник](diagrams/context-map.puml) |
| C4, уровень 1: окружение системы | [SVG](diagrams/rendered/c4-context.svg) · [исходник](diagrams/c4-context.puml) |
| C4, уровень 2: все контейнеры и синхронные связи | [SVG](diagrams/rendered/c4-containers.svg) · [исходник](diagrams/c4-containers.puml) |
| C4, уровень 2: асинхронные связи через Kafka | [SVG](diagrams/rendered/c4-events.svg) · [исходник](diagrams/c4-events.puml) |
| C4, уровень 2: владение хранилищами | [SVG](diagrams/rendered/c4-data.svg) · [исходник](diagrams/c4-data.puml) |
| C4, уровни 2–3 по сервисам: C2 сервиса и C3 каждого контейнера (API и каждого worker); встроены в документы сервисов [docs/services/](docs/services/) | SVG: [diagrams/rendered/services/](diagrams/rendered/services/)`<сервис>/` · исходники: [diagrams/services/](diagrams/services/)`<сервис>/` · сводная таблица ссылок: [§4.1 отчёта](docs/lab-01.md#41-контейнеры-c4-уровень-2-и-компоненты-уровень-3) |
| Запуск поездки (sequence) | [SVG](diagrams/rendered/trip-start-sequence.svg) · [исходник](diagrams/trip-start-sequence.puml) |
| Завершение поездки с фото (sequence) | [SVG](diagrams/rendered/trip-finish-sequence.svg) · [исходник](diagrams/trip-finish-sequence.puml) |
| Отрендеренные SVG всех схем | [diagrams/rendered/](diagrams/rendered/) |

Основное решение — восемь bounded contexts и девять независимо развёртываемых сервисов из 18 контейнеров приложений:

- Workers каждого сервиса — отдельные контейнеры со своей C3-диаграммой.
- Разблокировка входит в сценарий поездки; команды устройству принадлежат Fleet Service.
- Телеметрия отделена от управления парком.
- Фотофиксация парковки, повреждений и фото к обжалованию штрафов выделена в Photo Service: файлы загружаются напрямую в объектное хранилище, метаданные хранятся в PostgreSQL.
- Каждый сервис владеет своими данными. Поиск строит проекцию по событиям.

«5 млн» трактуется как число зарегистрированных пользователей; 5 000 RPS — суммарный пик внешнего клиентского API. Это допущения варианта, они и остальные оценки объёмов описаны в [§1 отчёта](docs/lab-01.md#1-исходные-требования-и-допущения).