"""п.5, п.7: raw-события → агрегаты → PostgreSQL → S3, пересчёт и экспорт идемпотентны."""
import csv
import io
import os
import uuid

import boto3
import requests

from conftest import AGGREGATION_URL, publish, wait_for_events

BUCKET = os.getenv("S3_BUCKET", "movie-analytics")


def event(user: str, movie: str, event_type: str, timestamp: str, progress: int | None = None) -> dict:
    return {
        "user_id": user,
        "movie_id": movie,
        "event_type": event_type,
        "timestamp": timestamp,
        "device_type": "DESKTOP",
        "session_id": f"{user}-{timestamp[:10]}",
        "progress_seconds": progress,
    }


def aggregate(day: str) -> dict:
    response = requests.post(f"{AGGREGATION_URL}/aggregate", params={"date": day}, timeout=60)
    assert response.status_code == 200, response.text
    return response.json()


def metrics(day: str) -> dict:
    response = requests.get(f"{AGGREGATION_URL}/metrics", params={"date": day}, timeout=30)
    assert response.status_code == 200, response.text
    return {(m["metric_name"], m["dimension"]): m["value"] for m in response.json()}


def test_metrics_are_computed_saved_and_exported(clickhouse):
    run = uuid.uuid4().hex[:8]
    alice, bob, carol, dave = (f"test-{name}-{run}" for name in ("alice", "bob", "carol", "dave"))
    events = [
        # 1 января: первый просмотр у alice и bob — это когорта 2001-01-01
        event(alice, "test-movie-a", "VIEW_STARTED", "2001-01-01T10:00:00Z", 0),
        event(alice, "test-movie-a", "VIEW_FINISHED", "2001-01-01T11:40:00Z", 6000),
        event(bob, "test-movie-a", "VIEW_STARTED", "2001-01-01T12:00:00Z", 0),
        # 2 января: alice вернулась, bob — нет; carol и dave — новые
        event(alice, "test-movie-a", "VIEW_STARTED", "2001-01-02T10:00:00Z", 0),
        event(alice, "test-movie-a", "VIEW_FINISHED", "2001-01-02T11:30:00Z", 5400),
        event(carol, "test-movie-b", "SEARCHED", "2001-01-02T12:00:00Z"),
        event(carol, "test-movie-b", "VIEW_STARTED", "2001-01-02T12:01:00Z", 0),
        event(dave, "test-movie-a", "VIEW_STARTED", "2001-01-02T13:00:00Z", 0),
        event(dave, "test-movie-a", "VIEW_FINISHED", "2001-01-02T14:00:00Z", 3600),
        # 8 января: alice зашла на 7-й день после первого просмотра
        event(alice, "test-movie-a", "LIKED", "2001-01-08T09:00:00Z"),
    ]
    for item in events:
        assert publish(item).status_code == 201
    wait_for_events(clickhouse, expected=len(events))

    # --- метрики за 2 января ---
    result = aggregate("2001-01-02")
    assert result["events_processed"] == 6
    expected = {
        ("dau", ""): 3,                          # alice, carol, dave
        ("views_started", ""): 3,
        ("views_finished", ""): 2,
        ("view_conversion_pct", ""): 66.67,      # 2 из 3
        ("avg_watch_seconds", ""): 4500,         # (5400 + 3600) / 2
        ("movie_views", "test-movie-a"): 2,
        ("movie_views", "test-movie-b"): 1,
        ("retention_d1_pct", ""): 50,            # из когорты {alice, bob} вернулась alice
    }
    assert metrics("2001-01-02") == expected

    # --- повторный пересчёт обновляет те же строки, а не добавляет новые ---
    aggregate("2001-01-02")
    assert metrics("2001-01-02") == expected

    # --- retention D7 за 8 января ---
    aggregate("2001-01-08")
    week_later = metrics("2001-01-08")
    assert week_later[("dau", "")] == 1
    assert week_later[("retention_d7_pct", "")] == 50

    # --- когортная таблица в ClickHouse: когорта 2001-01-01 ---
    cohort = clickhouse.query(
        "SELECT day_number, cohort_size, returned_users, retention_pct FROM agg_retention_cohorts FINAL "
        "WHERE cohort_date = '2001-01-01' AND day_number IN (1, 7) ORDER BY day_number"
    ).result_rows
    assert cohort == [(1, 2, 1, 50), (7, 2, 1, 50)]

    # --- экспорт в S3: один файл на дату, повторный экспорт его перезаписывает ---
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )
    for _ in range(2):
        response = requests.post(f"{AGGREGATION_URL}/export", params={"date": "2001-01-02"}, timeout=60)
        assert response.status_code == 200, response.text
        assert response.json()["path"] == f"s3://{BUCKET}/daily/2001-01-02/aggregates.csv"

    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="daily/2001-01-02/")
    assert [obj["Key"] for obj in listing["Contents"]] == ["daily/2001-01-02/aggregates.csv"]

    body = s3.get_object(Bucket=BUCKET, Key="daily/2001-01-02/aggregates.csv")["Body"].read().decode()
    rows = list(csv.DictReader(io.StringIO(body)))
    assert {(r["metric_name"], r["dimension"]): float(r["value"]) for r in rows} == expected
    assert all(r["metric_date"] == "2001-01-02" and r["computed_at"] for r in rows)


def test_export_without_aggregates_returns_404():
    response = requests.post(f"{AGGREGATION_URL}/export", params={"date": "1999-12-31"}, timeout=30)
    assert response.status_code == 404
