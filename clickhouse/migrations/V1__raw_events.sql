-- п.3: приём событий из Kafka и постоянное хранение.
-- Типы колонок повторяют Avro-схему schemas/movie_event.avsc:
--   string (uuid) → UUID, enum → Enum8, long (timestamp-millis) → DateTime64(3), ["null","int"] → Nullable(Int32)

-- 1. Очередь: таблица-потребитель топика movie-events. Данные в ней не хранятся.
--    AvroConfluent: по id схемы из сообщения ClickHouse берёт схему в Schema Registry.
CREATE TABLE IF NOT EXISTS cinema.movie_events_queue
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_flush_interval_ms = 1000;

-- 2. Постоянное хранение raw-событий.
--    PARTITION BY месяц: партиций немного, старые месяцы удаляются целиком (DROP PARTITION).
--    ORDER BY (день, тип события, пользователь): все агрегаты считаются за день, почти все фильтруют
--    по типу события, а DAU и retention считают уникальных пользователей.
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32),
    event_date       Date MATERIALIZED toDate(timestamp)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id);

-- 3. Автоматическая перекладка: каждый блок, прочитанный из Kafka, вставляется в movie_events.
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv TO cinema.movie_events AS
SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
FROM cinema.movie_events_queue;
