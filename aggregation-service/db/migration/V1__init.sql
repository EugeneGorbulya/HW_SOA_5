-- Хранилище готовых метрик: одна строка = одно значение метрики за день.
-- dimension — разрез метрики (movie_id для movie_views), у остальных метрик пустая строка.
CREATE TABLE daily_metrics (
    metric_date DATE           NOT NULL,
    metric_name TEXT           NOT NULL,
    dimension   TEXT           NOT NULL DEFAULT '',
    value       NUMERIC(18, 2) NOT NULL,
    computed_at TIMESTAMPTZ    NOT NULL,
    -- по этому ключу работает UPSERT: пересчёт за ту же дату обновляет строку, а не добавляет новую
    PRIMARY KEY (metric_date, metric_name, dimension)
);
