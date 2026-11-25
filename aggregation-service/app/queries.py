"""SQL для ClickHouse. Параметры {name:Type} подставляет сервер, а не Python.

Количество считаем как uniqExact(event_id), а не count(): доставка из Kafka — at-least-once,
и повторно доставленное событие не должно попасть в метрику дважды.
"""

EVENTS_IN_RANGE = """
SELECT uniqExact(event_id)
FROM movie_events
WHERE event_date BETWEEN {date_from:Date} AND {date_to:Date}
"""

# DAU, конверсия просмотра, среднее время просмотра
INSERT_DAILY_METRICS = """
INSERT INTO agg_daily_metrics
    (event_date, dau, views_started, views_finished, view_conversion_pct, avg_watch_seconds, computed_at)
SELECT
    event_date,
    uniqExact(user_id)                                    AS dau,
    uniqExactIf(event_id, event_type = 'VIEW_STARTED')    AS views_started,
    uniqExactIf(event_id, event_type = 'VIEW_FINISHED')   AS views_finished,
    if(views_started = 0, 0, round(100 * views_finished / views_started, 2))        AS view_conversion_pct,
    ifNull(round(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 2), 0)      AS avg_watch_seconds,
    {computed_at:DateTime('UTC')}                         AS computed_at
FROM movie_events
WHERE event_date BETWEEN {date_from:Date} AND {date_to:Date}
GROUP BY event_date
"""

# Рейтинг фильмов: место в рейтинге дня — оконная функция
INSERT_TOP_MOVIES = """
INSERT INTO agg_top_movies (event_date, movie_id, views, rank, computed_at)
SELECT
    event_date,
    movie_id,
    views,
    row_number() OVER (PARTITION BY event_date ORDER BY views DESC, movie_id) AS rank,
    {computed_at:DateTime('UTC')} AS computed_at
FROM
(
    SELECT event_date, movie_id, uniqExact(event_id) AS views
    FROM movie_events
    WHERE event_type = 'VIEW_STARTED' AND event_date BETWEEN {date_from:Date} AND {date_to:Date}
    GROUP BY event_date, movie_id
)
"""

# Когортный retention.
#   first_views — когорта пользователя: дата его первого просмотра (по всей истории);
#   activity    — в какие дни из пересчитываемого диапазона пользователь был активен (любое событие);
#   cohorts     — размер каждой когорты, размноженный на дни жизни 0..7;
#   returned    — сколько пользователей когорты были активны на N-й день.
# Пишутся только клетки, у которых день активности (cohort_date + day_number) попал в диапазон пересчёта:
# клетка «когорта 01.04, Day 3» считается при пересчёте за 04.04.
INSERT_RETENTION_COHORTS = """
INSERT INTO agg_retention_cohorts
    (cohort_date, day_number, cohort_size, returned_users, retention_pct, computed_at)
WITH
    first_views AS
    (
        SELECT user_id, min(event_date) AS cohort_date
        FROM movie_events
        WHERE event_type = 'VIEW_STARTED'
        GROUP BY user_id
    ),
    activity AS
    (
        SELECT DISTINCT user_id, event_date
        FROM movie_events
        WHERE event_date BETWEEN {date_from:Date} AND {date_to:Date}
    )
SELECT
    cohort_date,
    day_number,
    cohort_size,
    returned_users,
    round(100 * returned_users / cohort_size, 2) AS retention_pct,
    {computed_at:DateTime('UTC')} AS computed_at
FROM
(
    SELECT cohort_date, cohort_size, toUInt8(arrayJoin(range(8))) AS day_number
    FROM (SELECT cohort_date, uniqExact(user_id) AS cohort_size FROM first_views GROUP BY cohort_date)
) AS cohorts
LEFT JOIN
(
    SELECT cohort_date, toUInt8(dateDiff('day', cohort_date, event_date)) AS day_number, uniqExact(user_id) AS returned_users
    FROM activity
    INNER JOIN first_views USING (user_id)
    WHERE event_date BETWEEN cohort_date AND cohort_date + 7
    GROUP BY cohort_date, day_number
) AS returned USING (cohort_date, day_number)
WHERE cohort_date + day_number BETWEEN {date_from:Date} AND {date_to:Date}
"""

# ---- Чтение готовых агрегатов для выгрузки в PostgreSQL: (дата, метрика, разрез, значение) ----

SELECT_DAILY_METRICS = """
SELECT event_date, dau, views_started, views_finished, view_conversion_pct, avg_watch_seconds
FROM agg_daily_metrics FINAL
WHERE event_date BETWEEN {date_from:Date} AND {date_to:Date}
"""

SELECT_MOVIE_VIEWS = """
SELECT event_date, movie_id, views
FROM agg_top_movies FINAL
WHERE event_date BETWEEN {date_from:Date} AND {date_to:Date}
"""

# Retention D1/D7 как метрика дня: за дату X это доля когорты X-1 (X-7), вернувшаяся в день X
SELECT_RETENTION = """
SELECT cohort_date + day_number AS metric_date, day_number, retention_pct
FROM agg_retention_cohorts FINAL
WHERE day_number IN (1, 7) AND metric_date BETWEEN {date_from:Date} AND {date_to:Date}
"""
