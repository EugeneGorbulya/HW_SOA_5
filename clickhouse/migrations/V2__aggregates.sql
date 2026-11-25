-- п.5: агрегаты, которые Aggregation Service считает из movie_events (INSERT ... SELECT).
-- ReplacingMergeTree(computed_at): в ClickHouse нет UPSERT, поэтому повторный пересчёт просто вставляет
-- строку с тем же ключом сортировки и новым computed_at; при чтении с FINAL остаётся самая свежая.

-- Метрики дня
CREATE TABLE IF NOT EXISTS cinema.agg_daily_metrics
(
    event_date          Date,
    dau                 UInt64,   -- уникальные пользователи за день
    views_started       UInt64,
    views_finished      UInt64,
    view_conversion_pct Float64,  -- VIEW_FINISHED / VIEW_STARTED, %
    avg_watch_seconds   Float64,  -- средний progress_seconds у VIEW_FINISHED
    computed_at         DateTime('UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY event_date;

-- Рейтинг фильмов за день по числу просмотров (VIEW_STARTED)
CREATE TABLE IF NOT EXISTS cinema.agg_top_movies
(
    event_date  Date,
    movie_id    String,
    views       UInt64,
    rank        UInt32,
    computed_at DateTime('UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (event_date, movie_id);

-- Когортный retention: когорта = дата первого просмотра, day_number = 0..7 — день жизни когорты
CREATE TABLE IF NOT EXISTS cinema.agg_retention_cohorts
(
    cohort_date    Date,
    day_number     UInt8,
    cohort_size    UInt64,   -- пользователей в когорте
    returned_users UInt64,   -- из них были активны в день cohort_date + day_number
    retention_pct  Float64,
    computed_at    DateTime('UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_number);
