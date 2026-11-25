import os
import time

import clickhouse_connect
import pytest
import requests

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
AGGREGATION_URL = os.getenv("AGGREGATION_URL", "http://localhost:8001")

# Песочница: все тестовые события датированы январём 2001 года. Они не попадают в «живую» аналитику,
# а очищаются одной командой — удалением месячной партиции.
SANDBOX_PARTITION = "200101"


@pytest.fixture(scope="session")
def clickhouse():
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        username=os.getenv("CLICKHOUSE_USER", "cinema"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "cinema"),
        database=os.getenv("CLICKHOUSE_DATABASE", "cinema"),
    )
    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_sandbox(clickhouse):
    """До и после каждого теста песочница пуста — тесты можно запускать сколько угодно раз."""
    clickhouse.command(f"ALTER TABLE movie_events DROP PARTITION {SANDBOX_PARTITION}")
    yield
    clickhouse.command(f"ALTER TABLE movie_events DROP PARTITION {SANDBOX_PARTITION}")


def publish(event: dict) -> requests.Response:
    return requests.post(f"{PRODUCER_URL}/events", json=event, timeout=30)


def wait_for_events(clickhouse, expected: int, timeout: float = 60) -> int:
    """Ждёт, пока в песочнице ClickHouse появится expected событий (Kafka → Kafka Engine → MergeTree)."""
    query = f"SELECT count() FROM movie_events WHERE toYYYYMM(event_date) = {SANDBOX_PARTITION}"
    deadline = time.monotonic() + timeout
    count = 0
    while time.monotonic() < deadline:
        count = clickhouse.query(query).first_row[0]
        if count >= expected:
            return count
        time.sleep(0.5)
    raise AssertionError(f"ClickHouse has {count} sandbox events after {timeout} s, expected {expected}")
