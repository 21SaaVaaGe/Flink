# Flink SQL 2.2.1: настройки для Kafka → Flink → Kafka → MSSQL Writer → MSSQL

Версия документа: 27.10.2026. Цель — изменить **существующий** SQL job и получить задержку в секундах, наблюдаемую пропускную способность и предсказуемое восстановление. Ни один набор чисел не гарантирует SLO без измерений: число партиций Kafka, SQL, объём состояния, ресурсы сервера и скорость MSSQL пока неизвестны. Значения ниже — **стартовая конфигурация для проверки**, а не подтверждённые параметры вашей площадки.

**Маршрут данных:** входной Kafka topic → существующий Flink SQL job → выходной Kafka topic → отдельный MSSQL Writer → MSSQL. Если сейчас SQL job пишет прямо в MSSQL через JDBC, см. раздел 8: это отдельный вариант с другой семантикой отказов. Не запускайте оба sink одновременно для одной и той же записи.

## 1. Что собрать перед изменением

Сохраните исходный `.sql`, текущий `config.yaml`/Compose, версии образа и JAR, схему таблицы MSSQL, число партиций входного и выходного topics, ключ Kafka, `group.id`, число TaskManager и слотов, объём RAM/CPU, скорость входа (сообщения/с и байты/с), размеры сообщений, lag, p50/p95/p99 задержки, длительность checkpoint, время записи в MSSQL и план восстановления. Зафиксируйте также версию Docker Compose. До измерений нельзя обоснованно выбрать окончательные `parallelism`, память, частоту checkpoint и размер SQL batch.

Для каждого события нужен стабильный идентификатор (`event_id`) и правило повторной обработки: `INSERT` с уникальным ключом, `MERGE`/upsert по бизнес-ключу либо другая идемпотентная операция. Определите, какая задержка считается SLO: от времени создания события до появления строки в MSSQL, а не только до выхода из Flink.

## 2. Что поставить обязательно

| Место | Параметр | Стартовое значение / действие | Зачем |
|---|---|---|---|
| Образ | Flink | `flink:2.2.1-scala_2.12`, зафиксировать образ и JAR | Одинаковая версия на всех узлах |
| Flink | `jobmanager.rpc.address` | `jobmanager` в Compose | TaskManager и SQL Client находят JobManager |
| Flink | `jobmanager.memory.process.size` | `1600m` как начало | Ограничение памяти процесса JobManager |
| Flink | `taskmanager.memory.process.size` | `3072m` как начало | Ограничение памяти процесса TaskManager |
| Flink | `taskmanager.numberOfTaskSlots` | `2` как начало | Два слота на TaskManager |
| Flink | `parallelism.default` | `2` как начало | Начальный параллелизм, далее по партициям/метрикам |
| Flink | `execution.checkpointing.interval` | `10 s` как начало | Включает периодические checkpoint |
| Flink | `execution.checkpointing.mode` | `EXACTLY_ONCE` | Согласованное состояние Flink; итоговая гарантия зависит от sink/Writer |
| Flink | `execution.checkpointing.dir` | Общий долговечный URI | Состояние должно переживать сбой процесса |
| Flink | `execution.checkpointing.savepoint-dir` | Отдельный долговечный URI | Обновление и управляемый откат |
| Flink | `execution.checkpointing.timeout` | `2 min` как начало | Ограничение зависшего checkpoint |
| Flink | `execution.checkpointing.min-pause` | `5 s` как начало | Не перегружать job непрерывными snapshot |
| Flink | `execution.checkpointing.max-concurrent-checkpoints` | `1` | Не накладывать checkpoint друг на друга |
| Flink | `restart-strategy.type` | `exponential-delay` | Автоматическое восстановление временных ошибок |
| Kafka source | `properties.bootstrap.servers` | Доступные из контейнера DNS:порт | `localhost` внутри контейнера указывает на сам контейнер |
| Kafka source | `properties.group.id` | Постоянный уникальный ID job | Контроль чтения и восстановления |
| Kafka source | `scan.startup.mode` | Явно выбрать для **первого** запуска | Не начать случайно с конца или начала topic |
| Kafka sink | `sink.delivery-guarantee` | `exactly-once`, если требуется сквозная гарантия | Kafka-транзакции вместе с checkpoint |
| Kafka sink | `sink.transactional-id-prefix` | Уникальный префикс этого job | Не конфликтовать с другим producer |
| MSSQL Writer | Идемпотентная запись по ключу | Обязательное требование | Без неё повтор после сбоя может создать дубли |
| Мониторинг | Метрики Flink, Kafka lag, Writer, MSSQL | Включить до нагрузочного теста | Нужна локализация узкого места |

`EXACTLY_ONCE` в Flink **не означает** автоматически exactly-once строк в MSSQL. Гарантия получается только после проверки цепочки Kafka transaction → `read_committed` у Writer → идемпотентная запись/транзакция MSSQL → согласованное подтверждение offset Writer. Если Writer так не устроен, документируйте фактическую гарантию как at-least-once с дедупликацией.

## 3. Каталоги для офлайн-установки

Создайте рядом с Compose:

```text
deployment/
  compose.yaml
  Dockerfile
  lib/                  # только дополнительные JAR
  sql/job.sql           # копия существующего SQL, затем точечные правки
  prometheus.yml        # если Prometheus работает в этой сети
```

В `lib/` подготовьте совместимые с Flink 2.2.1 JAR для Kafka SQL connector, JDBC SQL connector **только если нужен прямой JDBC sink**, Microsoft JDBC driver **только если нужен JDBC sink** и `flink-metrics-prometheus`. Точные версии connector проверяйте по совместимости с Flink 2.2 и по вашим артефактам; не подменяйте их версиями из случайных примеров. Зафиксируйте SHA-256 каждого JAR и образа. Не монтируйте `./lib` целиком поверх `/opt/flink/lib`: так скроются штатные библиотеки Flink.

## 4. Dockerfile для полностью офлайн-сборки

Предварительно загрузите базовый образ из доверенного архива (`docker load -i flink-base.tar`) и проверьте его имя/тег. `Dockerfile` не скачивает зависимости:

```dockerfile
FROM flink:2.2.1-scala_2.12
COPY lib/*.jar /opt/flink/lib/
```

Соберите на машине, где уже есть базовый образ и JAR: `docker build --pull=false -t local/flink-sql:2.2.1 .`. Перенесите образ через `docker save`/`docker load`. Одинаковый образ должен использоваться JobManager, TaskManager и SQL Client. Если Prometheus тоже будет локальным, заранее загрузите и зафиксируйте его образ.

## 5. `compose.yaml`: готовый стартовый шаблон

Этот Compose запускает один JobManager и два TaskManager на **одном Docker-хосте**. Он годится для контролируемого запуска и измерения; при отказе хоста локальные volume и JobManager недоступны. Для production HA см. раздел 14. Перед запуском создайте `sql/job.sql` и замените сетевые адреса/секреты в нём.

```yaml
name: flink-sql-prod-check

x-flink-properties: &flink-properties |
  jobmanager.rpc.address: jobmanager
  rest.address: jobmanager
  jobmanager.memory.process.size: 1600m
  taskmanager.memory.process.size: 3072m
  taskmanager.numberOfTaskSlots: 2
  parallelism.default: 2
  execution.checkpointing.interval: 10 s
  execution.checkpointing.mode: EXACTLY_ONCE
  execution.checkpointing.storage: filesystem
  execution.checkpointing.dir: file:///opt/flink/state/checkpoints
  execution.checkpointing.savepoint-dir: file:///opt/flink/state/savepoints
  execution.checkpointing.timeout: 2 min
  execution.checkpointing.min-pause: 5 s
  execution.checkpointing.max-concurrent-checkpoints: 1
  restart-strategy.type: exponential-delay
  metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
  metrics.reporter.prom.port: 9249

services:
  jobmanager:
    image: local/flink-sql:2.2.1
    command: jobmanager
    restart: unless-stopped
    environment:
      FLINK_PROPERTIES: *flink-properties
    ports:
      - "127.0.0.1:8081:8081"
    mem_limit: 2g
    cpus: 1.0
    volumes:
      - flink-state:/opt/flink/state
    networks: [streaming]

  taskmanager:
    image: local/flink-sql:2.2.1
    command: taskmanager
    restart: unless-stopped
    depends_on: [jobmanager]
    deploy:
      replicas: 2
    environment:
      FLINK_PROPERTIES: *flink-properties
    mem_limit: 4g
    cpus: 2.0
    volumes:
      - flink-state:/opt/flink/state
    networks: [streaming]

  sql-client:
    image: local/flink-sql:2.2.1
    command: ["/opt/flink/bin/sql-client.sh"]
    profiles: ["tools"]
    depends_on: [jobmanager]
    environment:
      FLINK_PROPERTIES: *flink-properties
    volumes:
      - ./sql:/opt/flink/sql:ro
    networks: [streaming]

volumes:
  flink-state:

networks:
  streaming:
```

Проверьте `docker compose config`, затем `docker compose up -d jobmanager taskmanager` и убедитесь, что оба TaskManager зарегистрированы. Отправка существующего SQL: `docker compose run --rm sql-client /opt/flink/bin/sql-client.sh -f /opt/flink/sql/job.sql`. **Отправка из SQL Client в session cluster не обеспечивает автоматический повторный запуск job после полной потери JobManager/хоста**; для этого нужна продуманная HA-схема, хранение checkpoint и способ повторной отправки job. Обычный `restart: unless-stopped` перезапускает контейнер, но не доказывает восстановление job.

Если один JobManager и два TaskManager требуют больше CPU/RAM, чем есть у машины, уменьшайте число реплик/размеры **после** измерения, а не запускайте под системным swap. Сумма лимитов Compose и запас для ОС, Kafka, Writer и MSSQL должны помещаться в физические ресурсы. Память процесса Flink должна быть меньше лимита контейнера с запасом; не задавайте одновременно `*.memory.process.size` и `*.memory.flink.size` без расчёта модели памяти.

## 6. `config.yaml` для запуска на железе

На bare metal перенесите блок `x-flink-properties` в `/opt/flink/conf/config.yaml`, но замените `jobmanager.rpc.address` на адрес доступного JobManager и `file:///opt/flink/state/...` на общий долговечный URI. У всех JobManager/TaskManager должны быть одинаковые connector JAR и конфигурация. Лимиты `mem_limit`/`cpus` в Compose заменяются системными ограничениями и выделением ресурсов ОС. Разница среды не меняет SQL-семантику, но меняет DNS, пути, лимиты памяти, сигналы остановки, доступность volume и процедуру запуска. `localhost` в контейнере и на физическом сервере имеют разный смысл.

## 7. Изменения в существующем SQL job

Не переписывайте job «с нуля». Сначала снимите `SHOW CREATE TABLE`, весь DDL/DML и план, сохраните исходный файл и правьте только выявленные причины задержки/сбоев. Базовая форма опций:

```sql
-- В существующем Kafka source проверьте и задайте:
-- 'connector' = 'kafka',
-- 'topic' = '<INPUT_TOPIC>',
-- 'properties.bootstrap.servers' = '<BROKERS_REACHABLE_FROM_CONTAINER>',
-- 'properties.group.id' = '<STABLE_GROUP_ID>',
-- 'scan.startup.mode' = 'group-offsets',
-- 'format' = '<EXISTING_FORMAT>'

-- В существующем Kafka sink, если нужна транзакционная запись:
-- 'connector' = 'kafka',
-- 'topic' = '<OUTPUT_TOPIC>',
-- 'properties.bootstrap.servers' = '<BROKERS_REACHABLE_FROM_CONTAINER>',
-- 'sink.delivery-guarantee' = 'exactly-once',
-- 'sink.transactional-id-prefix' = '<UNIQUE_PREFIX>',
-- 'format' = '<EXISTING_FORMAT>'
```

Это **фрагменты опций**, не готовый DDL: нельзя угадать вашу схему, ключи, формат, watermark и правила преобразования. Для первого старта `group-offsets` подходит только если у группы уже есть корректные committed offsets; иначе согласуйте `earliest-offset` или `latest-offset` и проверьте replay на тестовом topic. При восстановлении из checkpoint стартовые опции не должны заменять состояние checkpoint. Если job выдаёт changelog (`UPDATE`/`DELETE`), обычный append Kafka sink может не соответствовать данным: проверьте необходимость upsert-kafka и первичного ключа.

Для Kafka exactly-once проверьте, что брокер допускает транзакции, `transaction.timeout.ms` не меньше максимальной длительности checkpoint + восстановления с запасом, а MSSQL Writer читает с `isolation.level=read_committed`. Уникальный transactional prefix нужен на каждый логический job. После изменения параллелизма и восстановления проверьте отсутствие конфликтов producer transaction. Граница гарантии — Flink → Kafka; MSSQL Writer проверяется отдельно.

## 8. Если сейчас job пишет прямо в MSSQL через JDBC

У Flink JDBC SQL sink есть `sink.buffer-flush.max-rows`, `sink.buffer-flush.interval`, `sink.max-retries`, `sink.parallelism`. Начните с текущих значений и измерьте, какую задержку добавляет буфер. Для SLO в секундах стартовая проба — `sink.buffer-flush.max-rows = '500'`, `sink.buffer-flush.interval = '1s'`, `sink.max-retries = '3'`; `sink.parallelism` сначала ограничьте допустимой нагрузкой MSSQL. Это **только проба**, а не гарантированный optimum. Без первичного ключа JDBC sink выполняет append, с подходящим ключом возможен upsert. Повтор после сбоя может повторно записать batch: обеспечьте уникальный ключ и идемпотентность. Прямой JDBC sink связывает backpressure Flink со скоростью MSSQL; отдельный Kafka → Writer позволяет измерять и масштабировать обе части независимо.

Пароли и сертификаты не помещайте в Git или общедоступный Compose. Храните защищённый SQL файл/секреты и ограничьте права доступа. Не рассчитывайте, что переменные среды Compose автоматически подставятся в текст `job.sql`: это надо отдельно реализовать и проверить. Для MSSQL укажите реальный host/port из контейнерной сети, TLS и проверку сертификата по требованиям вашей инфраструктуры.

Если используется отдельный **MSSQL Writer**, задайте в нём и проверьте следующие параметры (имена ключей зависят от вашей реализации):

| Узел | Обязательная настройка/проверка | Старт для теста |
|---|---|---|
| Kafka consumer | `bootstrap.servers`, `group.id`, выходной topic | Постоянная группа Writer; адреса доступны из его среды |
| Kafka consumer | `isolation.level=read_committed` | Обязательно для транзакционного Kafka sink |
| Kafka consumer | `enable.auto.commit=false` | Offset подтверждается после успешного commit MSSQL |
| Kafka consumer | `auto.offset.reset` | Явно согласовать только для первого запуска без offset |
| Kafka consumer | `max.poll.records` | `500` как первая проба; измерить память и время batch |
| Kafka consumer | `max.poll.interval.ms` | Больше худшего времени batch с ретраями и запасом |
| MSSQL | Уникальный индекс/ключ `event_id` или бизнес-ключ | Реальная идемпотентность повторной записи |
| MSSQL | Транзакция batch и порядок подтверждения offset | Сначала commit MSSQL, затем commit Kafka offset |
| MSSQL | Тайм-аут подключения/запроса, retry с паузой | Не держать бесконечно зависший batch |
| Writer | Лимит batch, flush interval, число воркеров | `500` записей / `1 s` / `1` воркер как проба |
| Writer | DLQ и политика poison records | Учитывать запись в DLQ до подтверждения offset |
| Writer | Метрики lag, batch duration, retry, ошибки | Экспортировать в Prometheus/журналы |

Порядок «commit MSSQL → commit Kafka offset» допускает повтор после падения между действиями; уникальный ключ/идемпотентный upsert устраняет дубли. Обратный порядок может потерять запись. Если Writer уже реализует транзакционный outbox или другую подтверждённую схему, используйте её фактическую гарантию и не меняйте механизм без анализа кода.

## 9. Параллелизм и пропускная способность

Начальные `parallelism.default: 2`, два TaskManager × два слота дают четыре доступных слота, но оптимум зависит от графа job. Kafka source нельзя эффективно распараллелить выше числа входных партиций. Увеличение параллелизма downstream может повысить конкуренцию за MSSQL и ухудшить latency. Измерьте по операторам `numRecordsIn/Out`, busy/backpressured/idle time, Kafka lag, throughput Writer, размер batch и время commit MSSQL. Цель — устойчивый throughput выше скорости входа с запасом, а не максимум строк в одном всплеске.

Если видите примерно 12 тыс. строк раз в несколько минут, проверьте по порядку: действительно ли сообщения приходят равномерно; не образуется ли lag в Kafka; не стоит ли длительный temporal/window/watermark оператор; не ограничен ли sink/Writer; не зависают ли checkpoint; не скрывает ли мониторинг промежуточные записи. Сравнение с миллионной Spark micro-batch некорректно без времени накопления и реальной сквозной задержки.

## 10. Checkpoint, состояние и восстановление

Локальный `flink-state` volume в Compose сохраняет состояние при перезапуске контейнера **на том же хосте**, но не является надёжным хранилищем при потере диска/хоста. Для production используйте доступный всем узлам отказоустойчивый URI (HDFS/S3-совместимое хранилище и соответствующий Flink filesystem plugin), отдельные пути для checkpoint/savepoint, резервирование и контроль прав. Не удаляйте checkpoint вручную до подтверждённого восстановления.

Измерьте длительность и размер checkpoint. Если checkpoint регулярно превышает интервал, растёт в размере или не завершается, выясните причину до уменьшения интервала. Испытайте: падение одного TaskManager; падение JobManager; потерю соединения Kafka; остановку Writer; недоступность MSSQL; повторный запуск с последнего успешного checkpoint/savepoint. Зафиксируйте RTO и число повторов/дублей.

## 11. Мониторинг: что включить и на что смотреть

`flink-metrics-prometheus` должен находиться в `/opt/flink/lib/` всех Flink контейнеров. В Compose уже включены `metrics.reporter.prom.factory.class` и порт `9249`. У Prometheus в одной сети с Compose пример targets:

```yaml
scrape_configs:
  - job_name: flink-jobmanager
    static_configs:
      - targets: ['jobmanager:9249']
  - job_name: flink-taskmanager
    dns_sd_configs:
      - names: ['taskmanager']
        type: A
        port: 9249
```

При масштабировании TaskManager проверьте, что service discovery действительно видит **каждый** экземпляр, а не только один адрес; если нет, используйте discovery вашей платформы или явные targets. Имена метрик и labels проверьте на реальном `/metrics`, потому что формат экспорта зависит от версии и конфигурации reporter. Готовые шаблоны из ранее подготовленного набора: [monitoring](flink-offline-kit/monitoring/).

Панель и алерты должны показывать: статус job/рестарты, Kafka input/output lag, records/sec по source/sink, busy/backpressure/idle, длительность/возраст/число неуспешных checkpoint, размер состояния, CPU/RAM/GC каждого контейнера, ошибки Kafka transaction, Writer lag и retries, MSSQL deadlocks/lock waits/CPU/IO/transaction log, а также p50/p95/p99 сквозной задержки по `event_id`/timestamp. Не объявляйте SLO выполненным по метрике Flink в отрыве от MSSQL.

## 12. SLO и алерты

Задайте измеримое SLO после согласования с владельцем данных. Пример для проверки: p95 «Kafka input → строка в MSSQL» ≤ 5 секунд при нормальной нагрузке; p99 ≤ 15 секунд; доступность обработки ≥ 99.9% за месяц; ноль необъяснённых потерь; восстановление после сбоя узла в пределах согласованного RTO. Это **пример**, а не заявленное требование пользователя.

Первичные алерты: нет успешного checkpoint дольше 3× ожидаемого интервала; job не `RUNNING`; lag растёт 5–10 минут при стабильном входе; backpressure sink устойчиво высокий; Writer не подтверждает прогресс; MSSQL batch latency превышает бюджет; контейнер рестартует/упирается в memory limit. Пороги задавайте по базовой линии после нагрузочного теста, чтобы не получить постоянный шум.

## 13. Приёмочные испытания перед переключением

1. `docker compose config` проходит; локальный образ и все JAR доступны офлайн; SQL Client видит нужные connector.
2. SQL job стартует из сохранённого файла и выполняет запрос без изменения бизнес-смысла.
3. Контрольный набор с известными `event_id` проходит до MSSQL без потерь и с ожидаемыми значениями.
4. Нагрузка не ниже типичного пика и всплеск ≥ 2×; измерены throughput, lag, p95/p99 сквозной задержки, размер checkpoint и использование ресурсов.
5. Остановлены по одному TaskManager, Writer и MSSQL; после восстановления нет необъяснённых пропусков/дублей.
6. Проверены schema evolution, пустые/битые сообщения, poison records и путь DLQ с возможностью повтора.
7. Проверен откат на старый SQL/образ и восстановление из заранее созданного savepoint; прогон выполнен в тестовой среде.

Не переключайте production только по успешному `docker compose up`: он подтверждает старт контейнеров, а не обработку и гарантии данных.

## 14. Production-развёртывание и HA

Однохостовый Compose из раздела 5 не обеспечивает HA. Для production разместите JobManager/TaskManager на нескольких узлах под платформой, поддерживающей восстановление, используйте отказоустойчивое хранилище checkpoint/savepoint и документированный процесс повторной отправки SQL job либо deployment mode, который управляет жизненным циклом job. Потребуются настройки HA, совместимые с выбранным resource provider (standalone/Kubernetes), и проверка на реальном кластере. Выбор ZooKeeper/Kubernetes HA, `high-availability.*`, число реплик и URI нельзя безопасно заполнить без вашей платформы. Не переносите `file:///opt/flink/state` на разные хосты: это разные файловые системы.

Контейнеры и bare metal можно сделать функционально эквивалентными, если совпадают версия Flink/JAR, конфигурация, сеть, секреты, хранилище состояния и лимиты. Контейнер добавляет слой изоляции ресурсов и воспроизводимую поставку; bare metal требует вручную поддерживать те же версии и конфиги. Сам по себе Docker не ускоряет Flink SQL и не решает ограничение MSSQL.

## 15. Офлайн-поставка и запуск

На связанной машине подготовьте и проверьте набор: Flink base/custom image, дополнительные JAR, при необходимости Prometheus image, SQL файл, Compose, контрольные SHA-256, инструкцию восстановления. Перенесите архивы через утверждённый канал. На офлайн-хосте выполните `docker load`, `docker image inspect local/flink-sql:2.2.1`, сверку SHA-256, `docker compose config`, затем запуск. В Dockerfile и Compose не должно быть шага, требующего Internet (`apt`, `curl`, `wget`, `docker pull`). Проверяйте также DNS/маршрут/сертификат до внешних Kafka и MSSQL из **контейнера**, а не только с хоста.

Версии образов и JAR сохраняйте вместе с SQL и файлом конфигурации. Изменение одного connector без повторной проверки совместимости и recovery теста не считается безопасным обновлением.

## 16. Что нужно подставить для окончательного production-конфига

Вышеприведённый файл можно использовать сейчас для стендового запуска. Чтобы выдать точные итоговые значения без догадок, нужны: существующий `job.sql`; текущий Compose или способ запуска на железе; версии Flink и всех connector; число Kafka партиций и скорость/размеры сообщений; схема MSSQL и ключи; код/настройки MSSQL Writer; ресурсы серверов; доступное хранилище checkpoint; реальные требования p95/p99/RTO/RPO и политика replay. После получения этих данных надо изменить **именно ваш** SQL и Compose, прогнать раздел 13 и закрепить измеренные значения.

### Официальная документация для проверки

- [Flink 2.2 Docker](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/resource-providers/standalone/docker/)
- [Flink 2.2 configuration](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/config/)
- [Flink 2.2 memory](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/memory/mem_setup/)
- [Flink 2.2 Kafka SQL connector](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/kafka/)
- [Flink 2.2 JDBC SQL connector](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/jdbc/)
- [Flink 2.2 checkpoints](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/checkpoints/)
- [Flink 2.2 metric reporters](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/metric_reporters/)
- [Локальная копия документации Flink 2.2](flink-offline-kit/official-docs-2.2/)
