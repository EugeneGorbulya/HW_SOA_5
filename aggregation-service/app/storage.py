import logging
import time
from datetime import date, datetime

import psycopg

from app import config

logger = logging.getLogger("storage")

# Идемпотентная запись: строка с таким (дата, метрика, разрез) уже есть → обновляем значение и время вычисления
UPSERT = """
INSERT INTO daily_metrics (metric_date, metric_name, dimension, value, computed_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (metric_date, metric_name, dimension)
DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
"""

SELECT_BY_DATE = """
SELECT metric_date, metric_name, dimension, value, computed_at
FROM daily_metrics
WHERE metric_date = %s
ORDER BY metric_name, dimension
"""


def save_metrics(metrics: list[tuple], computed_at: datetime) -> None:
    """Все метрики цикла — одной транзакцией. PostgreSQL недоступен → retry с экспоненциальной паузой."""
    rows = [(*metric, computed_at) for metric in metrics]
    for attempt in range(1, config.POSTGRES_MAX_ATTEMPTS + 1):
        try:
            # выход из with без ошибки = COMMIT, с ошибкой = ROLLBACK
            with psycopg.connect(config.POSTGRES_DSN, connect_timeout=5) as conn:
                conn.cursor().executemany(UPSERT, rows)
            return
        except psycopg.OperationalError as exc:
            if attempt == config.POSTGRES_MAX_ATTEMPTS:
                logger.error("PostgreSQL write failed after %d attempts: %s", attempt, exc)
                raise
            delay_ms = config.POSTGRES_RETRY_BASE_DELAY_MS * 2 ** (attempt - 1)
            logger.warning(
                "PostgreSQL write failed, attempt %d/%d: %s; retrying in %d ms",
                attempt, config.POSTGRES_MAX_ATTEMPTS, str(exc).strip(), delay_ms,
            )
            time.sleep(delay_ms / 1000)


def fetch_metrics(day: date) -> list[tuple]:
    with psycopg.connect(config.POSTGRES_DSN, connect_timeout=5) as conn:
        return conn.execute(SELECT_BY_DATE, (day,)).fetchall()
