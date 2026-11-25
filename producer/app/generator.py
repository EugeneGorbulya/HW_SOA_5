"""Генератор синтетических событий: имитирует пользователей онлайн-кинотеатра.

Единица генерации — сессия просмотра одного фильма:
    [SEARCHED] → VIEW_STARTED → (VIEW_PAUSED → VIEW_RESUMED)* → [VIEW_FINISHED → [LIKED]]
Время событий внутри сессии только растёт, progress_seconds не убывает.
"""
import logging
import random
import threading
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

from app import config
from app.publisher import EventPublisher, PublishError

logger = logging.getLogger("generator")

# Каталог: (movie_id, длительность в секундах). Первые фильмы популярнее (вес 1/n)
MOVIES = [(f"movie-{n:03d}", random.Random(n).randint(80, 150) * 60) for n in range(1, 41)]
MOVIE_WEIGHTS = [1 / n for n in range(1, 41)]
DEVICES = ["MOBILE", "DESKTOP", "TV", "TABLET"]
DEVICE_WEIGHTS = [0.4, 0.25, 0.25, 0.1]

FINISH_PROBABILITY = 0.6   # досматривают до конца
SEARCH_PROBABILITY = 0.3   # фильм нашли через поиск
LIKE_PROBABILITY = 0.35    # лайк после просмотра
NEW_USER_SHARE = 0.2       # доля новых пользователей в live-режиме


@dataclass
class User:
    user_id: str
    device: str
    joined: date


def new_user(joined: date) -> User:
    return User(f"user-{uuid.uuid4().hex[:8]}", random.choices(DEVICES, DEVICE_WEIGHTS)[0], joined)


def return_probability(days_since_join: int) -> float:
    """Вероятность, что пользователь зайдёт через N дней после первого просмотра: 45% на D1, дальше затухает."""
    return 0.45 * 0.85 ** (days_since_join - 1)


def build_session(user: User, started_at: datetime) -> list[dict]:
    movie_id, duration = random.choices(MOVIES, MOVIE_WEIGHTS)[0]
    session_id = str(uuid.uuid4())
    events = []
    now = started_at
    progress = 0

    def add(event_type: str, progress_seconds: int | None = None) -> None:
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user.user_id,
            "movie_id": movie_id,
            "event_type": event_type,
            "timestamp": now,
            "device_type": user.device,
            "session_id": session_id,
            "progress_seconds": progress_seconds,
        })

    if random.random() < SEARCH_PROBABILITY:
        add("SEARCHED")
        now += timedelta(seconds=random.randint(5, 60))

    add("VIEW_STARTED", progress)

    finishes = random.random() < FINISH_PROBABILITY
    # досмотрит до конца или бросит где-то в первых 70% фильма
    watch_until = duration if finishes else random.randint(60, int(duration * 0.7))

    for _ in range(random.randint(0, 2)):
        watched = random.randint(60, 1800)
        if progress + watched >= watch_until:
            break
        progress += watched
        now += timedelta(seconds=watched)
        add("VIEW_PAUSED", progress)
        now += timedelta(seconds=random.randint(10, 600))
        add("VIEW_RESUMED", progress)

    if finishes:
        now += timedelta(seconds=watch_until - progress)
        add("VIEW_FINISHED", watch_until)
        if random.random() < LIKE_PROBABILITY:
            now += timedelta(seconds=random.randint(2, 30))
            add("LIKED")
    return events


class EventGenerator:
    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher
        self._users: list[User] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="generator", daemon=True)
        self.state = "starting"

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=10)

    def _run(self) -> None:
        try:
            self._backfill()
            self._live()
        except Exception:
            logger.exception("generator crashed")
            self.state = "crashed"

    def _publish_session(self, events: list[dict]) -> int:
        for event in events:
            if self._stop.is_set():
                return 0
            try:
                self._publisher.publish(event)
            except PublishError:
                # остаток сессии не шлём: VIEW_FINISHED без VIEW_STARTED хуже, чем пропавшая сессия
                logger.error("session %s dropped: Kafka is unavailable", event["session_id"])
                return 0
        return len(events)

    def _backfill(self) -> None:
        """История за прошлые дни: каждый день приходят новые пользователи и возвращается часть старых."""
        if not config.GENERATOR_BACKFILL_DAYS:
            return
        # историю пишем только в пустой топик: перезапуск сервиса не должен создавать её второй раз
        if not self._publisher.topic_is_empty():
            logger.info("backfill skipped: topic already has events")
            return
        self.state = "backfill"
        today = datetime.now(timezone.utc).date()
        published = 0
        for days_ago in range(config.GENERATOR_BACKFILL_DAYS, 0, -1):
            day = today - timedelta(days=days_ago)
            returned = [u for u in self._users if random.random() < return_probability((day - u.joined).days)]
            newcomers = [new_user(day) for _ in range(config.GENERATOR_NEW_USERS_PER_DAY)]
            self._users += newcomers
            for user in returned + newcomers:
                # сессия начинается в первой половине суток и до полуночи успевает закончиться
                started_at = datetime.combine(day, time(0, 0), timezone.utc) + timedelta(seconds=random.randint(0, 12 * 3600))
                for _ in range(random.choice([1, 1, 2])):
                    events = build_session(user, started_at)
                    published += self._publish_session(events)
                    started_at = events[-1]["timestamp"] + timedelta(seconds=random.randint(60, 1800))
            logger.info("backfill %s: %d returned + %d new users", day, len(returned), len(newcomers))
        logger.info("backfill finished: %d events for %d days", published, config.GENERATOR_BACKFILL_DAYS)

    def _live(self) -> None:
        """Поток событий за сегодня: раз в несколько секунд у кого-то заканчивается сессия."""
        self.state = "live"
        day = None
        waiting: list[User] = []
        while not self._stop.is_set():
            now = datetime.now(timezone.utc)
            if now.date() != day:
                # наступил новый день — решаем, кто из старых пользователей сегодня вернётся
                day = now.date()
                waiting = [u for u in self._users if random.random() < return_probability(max((day - u.joined).days, 1))]
                random.shuffle(waiting)
            if waiting and random.random() > NEW_USER_SHARE:
                user = waiting.pop()
            else:
                user = new_user(day)
                self._users.append(user)
            # сессия закончилась только что: сдвигаем её так, чтобы последнее событие было «сейчас»
            events = build_session(user, now)
            shift = now - events[-1]["timestamp"]
            for event in events:
                event["timestamp"] += shift
            self._publish_session(events)
            self._stop.wait(60 / config.GENERATOR_SESSIONS_PER_MINUTE)
