"""п.4: путь события HTTP API продюсера → Kafka → ClickHouse."""
import uuid
from datetime import date, datetime, timezone

import pytest

from conftest import publish, wait_for_events

SELECT_EVENT = """
SELECT event_id, user_id, movie_id, event_type, toUnixTimestamp64Milli(timestamp), device_type,
       session_id, progress_seconds, event_date
FROM movie_events
WHERE event_id = {event_id:UUID}
"""


def make_event(**overrides) -> dict:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"test-user-{uuid.uuid4().hex[:8]}",
        "movie_id": "test-movie-a",
        "event_type": "VIEW_FINISHED",
        "timestamp": "2001-01-20T10:15:30.123Z",
        "device_type": "TV",
        "session_id": str(uuid.uuid4()),
        "progress_seconds": 5400,
    }
    event.update(overrides)
    return event


def test_event_reaches_clickhouse_with_correct_fields(clickhouse):
    event = make_event()

    response = publish(event)
    assert response.status_code == 201, response.text
    assert response.json() == {"event_id": event["event_id"]}

    wait_for_events(clickhouse, expected=1)
    rows = clickhouse.query(SELECT_EVENT, parameters={"event_id": event["event_id"]}).result_rows
    assert len(rows) == 1
    assert rows[0] == (
        uuid.UUID(event["event_id"]),
        event["user_id"],
        "test-movie-a",
        "VIEW_FINISHED",
        int(datetime(2001, 1, 20, 10, 15, 30, 123000, tzinfo=timezone.utc).timestamp() * 1000),
        "TV",
        event["session_id"],
        5400,
        date(2001, 1, 20),
    )


def test_event_without_progress_is_stored_as_null(clickhouse):
    # у LIKED нет progress_seconds, event_id сервис генерирует сам
    event = make_event(event_type="LIKED", device_type="MOBILE")
    del event["event_id"], event["progress_seconds"]

    response = publish(event)
    assert response.status_code == 201, response.text
    event_id = response.json()["event_id"]

    wait_for_events(clickhouse, expected=1)
    rows = clickhouse.query(SELECT_EVENT, parameters={"event_id": event_id}).result_rows
    assert len(rows) == 1
    assert rows[0][3] == "LIKED"
    assert rows[0][7] is None


@pytest.mark.parametrize(
    "broken",
    [
        {"event_type": "VIEW_REWOUND"},     # нет такого значения в enum
        {"device_type": "FRIDGE"},
        {"progress_seconds": None},         # для VIEW_* прогресс обязателен
        {"progress_seconds": -5},
        {"user_id": ""},
        {"event_id": "not-a-uuid"},
        {"timestamp": "yesterday"},
        {"unknown_field": 1},
    ],
    ids=[
        "unknown event_type", "unknown device_type", "view without progress", "negative progress",
        "empty user_id", "event_id is not uuid", "bad timestamp", "extra field",
    ],
)
def test_invalid_event_is_rejected(broken):
    response = publish(make_event(**broken))
    assert response.status_code == 422, response.text
