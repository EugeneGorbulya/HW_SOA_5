import logging
import threading
import time
from datetime import date, datetime, timezone

import clickhouse_connect

from app import config, queries, storage

logger = logging.getLogger("aggregation")

# Плановый и ручной пересчёт не выполняются одновременно
_lock = threading.Lock()

Metric = tuple[date, str, str, float]  # (дата, метрика, разрез, значение)


def run_cycle(date_from: date, date_to: date, trigger: str) -> dict:
    """Один цикл: raw-события ClickHouse → агрегаты в ClickHouse → UPSERT в PostgreSQL."""
    with _lock:
        started = time.perf_counter()
        logger.info("aggregation cycle started (%s): %s .. %s", trigger, date_from, date_to)

        # одно время вычисления на весь цикл — и в ClickHouse, и в PostgreSQL
        computed_at = datetime.now(timezone.utc).replace(microsecond=0)
        params = {"date_from": date_from, "date_to": date_to, "computed_at": computed_at.replace(tzinfo=None)}

        client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST, port=config.CLICKHOUSE_PORT,
            username=config.CLICKHOUSE_USER, password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DATABASE,
        )
        try:
            events = client.query(queries.EVENTS_IN_RANGE, parameters=params).first_row[0]
            for insert in (queries.INSERT_DAILY_METRICS, queries.INSERT_TOP_MOVIES, queries.INSERT_RETENTION_COHORTS):
                client.command(insert, parameters=params)
            metrics = _collect_metrics(client, params)
        finally:
            client.close()

        storage.save_metrics(metrics, computed_at)

        duration = time.perf_counter() - started
        logger.info(
            "aggregation cycle finished (%s): %s .. %s, processed %d events, wrote %d metrics in %.2f s",
            trigger, date_from, date_to, events, len(metrics), duration,
        )
        return {
            "date_from": date_from,
            "date_to": date_to,
            "events_processed": events,
            "metrics_written": len(metrics),
            "duration_seconds": round(duration, 3),
            "computed_at": computed_at,
        }


def _collect_metrics(client, params: dict) -> list[Metric]:
    """Читает только что посчитанные агрегаты и раскладывает их в строки для PostgreSQL."""
    metrics: list[Metric] = []

    rows = client.query(queries.SELECT_DAILY_METRICS, parameters=params).result_rows
    for day, dau, views_started, views_finished, conversion, avg_watch in rows:
        metrics += [
            (day, "dau", "", dau),
            (day, "views_started", "", views_started),
            (day, "views_finished", "", views_finished),
            (day, "view_conversion_pct", "", conversion),
            (day, "avg_watch_seconds", "", avg_watch),
        ]

    rows = client.query(queries.SELECT_MOVIE_VIEWS, parameters=params).result_rows
    for day, movie_id, views in rows:
        metrics.append((day, "movie_views", movie_id, views))

    rows = client.query(queries.SELECT_RETENTION, parameters=params).result_rows
    for day, day_number, retention in rows:
        metrics.append((day, f"retention_d{day_number}_pct", "", retention))

    return metrics
