import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from app import config
from app.generator import EventGenerator
from app.publisher import EventPublisher, InvalidEventError, PublishError
from app.schemas import EventIn, EventOut

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("movie-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = EventPublisher()
    generator = EventGenerator(publisher) if config.GENERATOR_ENABLED else None
    app.state.publisher = publisher
    app.state.generator = generator
    if generator:
        generator.start()
    logger.info(
        "Kafka at %s, topic=%s, generator=%s (backfill %d days, %g sessions/min)",
        config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_TOPIC, "on" if generator else "off",
        config.GENERATOR_BACKFILL_DAYS, config.GENERATOR_SESSIONS_PER_MINUTE,
    )
    yield
    if generator:
        generator.stop()
    publisher.close()


app = FastAPI(title="Movie Service (producer)", version="1.0.0", lifespan=lifespan)


# Обычный def: FastAPI выполняет его в пуле потоков, ожидание подтверждения от Kafka не блокирует event loop
@app.post("/events", status_code=201, response_model=EventOut)
def publish_event(event: EventIn, request: Request) -> EventOut:
    """Принимает событие, проверяет его и публикует в Kafka. Отвечает после подтверждения записи брокерами."""
    try:
        request.app.state.publisher.publish(event.to_record())
    except InvalidEventError as exc:
        raise HTTPException(status_code=422, detail=f"event does not match Avro schema: {exc}")
    except PublishError as exc:
        raise HTTPException(status_code=503, detail=f"Kafka is unavailable: {exc}")
    return EventOut(event_id=event.event_id)


@app.get("/health")
def health(request: Request) -> dict:
    generator = request.app.state.generator
    return {"status": "ok", "generator": generator.state if generator else "disabled"}
