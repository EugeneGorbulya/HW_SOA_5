import csv
import io
import logging
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app import config, storage

logger = logging.getLogger("export")

# Даты, которые плановый экспорт ещё не выгрузил: вчерашняя и все, на которых прошлые запуски упали
_pending: set[date] = set()


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 2}),
    )


def _ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=config.S3_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=config.S3_BUCKET)
        logger.info("bucket %s created", config.S3_BUCKET)


def export_day(day: date) -> dict | None:
    """Метрики за день из PostgreSQL → s3://<bucket>/daily/YYYY-MM-DD/aggregates.csv. None — метрик за день нет."""
    rows = storage.fetch_metrics(day)
    if not rows:
        logger.warning("export %s: no aggregates in PostgreSQL, nothing to export", day)
        return None

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["metric_date", "metric_name", "dimension", "value", "computed_at"])
    for metric_date, name, dimension, value, computed_at in rows:
        writer.writerow([metric_date.isoformat(), name, dimension, value, computed_at.astimezone(timezone.utc).isoformat()])

    key = f"daily/{day.isoformat()}/aggregates.csv"
    s3 = _s3_client()
    _ensure_bucket(s3)
    # ключ объекта зависит только от даты: повторный экспорт перезаписывает файл
    s3.put_object(Bucket=config.S3_BUCKET, Key=key, Body=buffer.getvalue().encode(), ContentType="text/csv")

    path = f"s3://{config.S3_BUCKET}/{key}"
    logger.info("export %s: %d rows written to %s", day, len(rows), path)
    return {"date": day, "path": path, "rows": len(rows)}


def scheduled_export() -> None:
    """Плановый запуск: выгружает вчерашний день и повторяет даты, которые не удалось выгрузить раньше."""
    _pending.add(datetime.now(timezone.utc).date() - timedelta(days=1))
    for day in sorted(_pending):
        try:
            export_day(day)
        except Exception as exc:
            logger.error("export %s failed: %s; will retry on the next run", day, exc)
            continue
        _pending.discard(day)
