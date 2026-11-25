import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query

from app import aggregation, config, export, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger("aggregation-service")


def scheduled_aggregation() -> None:
    today = datetime.now(timezone.utc).date()
    try:
        aggregation.run_cycle(today - timedelta(days=config.AGGREGATION_LOOKBACK_DAYS), today, "schedule")
    except Exception as exc:
        logger.error("aggregation cycle failed: %s; next run in %d s", exc, config.AGGREGATION_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Задачи выполняются в потоках планировщика, HTTP-запросам не мешают.
    # misfire_grace_time=None: запуск, пропущенный вовремя (машина спала, контейнер стоял на паузе), выполняется
    # сразу, как только планировщик проснётся, а не отбрасывается; coalesce — несколько пропущенных запусков
    # одной задачи сливаются в один
    scheduler = BackgroundScheduler(timezone="UTC", job_defaults={"misfire_grace_time": None, "coalesce": True})
    scheduler.add_job(
        scheduled_aggregation, "interval", seconds=config.AGGREGATION_INTERVAL_SECONDS,
        next_run_time=datetime.now(timezone.utc),  # первый цикл — сразу при старте
    )
    scheduler.add_job(export.scheduled_export, CronTrigger.from_crontab(config.EXPORT_CRON, timezone="UTC"))
    scheduler.start()
    logger.info(
        "schedule: aggregation every %d s for the last %d days, S3 export by cron '%s' (UTC)",
        config.AGGREGATION_INTERVAL_SECONDS, config.AGGREGATION_LOOKBACK_DAYS, config.EXPORT_CRON,
    )
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Aggregation Service", version="1.0.0", lifespan=lifespan)


@app.post("/aggregate")
def aggregate(day: date = Query(alias="date", description="Дата пересчёта, YYYY-MM-DD")) -> dict:
    """Ручной пересчёт агрегатов за дату, не дожидаясь расписания."""
    try:
        return aggregation.run_cycle(day, day, "manual")
    except Exception as exc:
        logger.error("manual aggregation for %s failed: %s", day, exc)
        raise HTTPException(status_code=503, detail=f"aggregation failed: {exc}")


@app.post("/export")
def export_to_s3(day: date = Query(alias="date", description="Дата экспорта, YYYY-MM-DD")) -> dict:
    """Ручной экспорт метрик за дату из PostgreSQL в S3."""
    try:
        result = export.export_day(day)
    except Exception as exc:
        logger.error("manual export for %s failed: %s", day, exc)
        raise HTTPException(status_code=503, detail=f"export failed: {exc}")
    if result is None:
        raise HTTPException(status_code=404, detail=f"no aggregates for {day}")
    return result


@app.get("/metrics")
def metrics(day: date = Query(alias="date", description="Дата, YYYY-MM-DD")) -> list[dict]:
    """Готовые метрики за дату из PostgreSQL."""
    try:
        rows = storage.fetch_metrics(day)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL is unavailable: {exc}")
    return [
        {"metric_date": metric_date, "metric_name": name, "dimension": dimension, "value": float(value), "computed_at": computed_at}
        for metric_date, name, dimension, value, computed_at in rows
    ]


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
