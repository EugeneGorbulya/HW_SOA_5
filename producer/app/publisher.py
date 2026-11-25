import json
import logging
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastavro import parse_schema
from fastavro.validation import ValidationError, validate

from app import config

logger = logging.getLogger("publisher")


class InvalidEventError(Exception):
    """Событие не проходит Avro-схему."""


class PublishError(Exception):
    """Kafka не подтвердила запись после всех попыток."""


class EventPublisher:
    def __init__(self) -> None:
        schema_str = Path(config.SCHEMA_PATH).read_text()
        self._schema = parse_schema(json.loads(schema_str))
        self._subject = f"{config.KAFKA_TOPIC}-value"

        # п.1: регистрируем схему в Schema Registry. Та же схема повторно → тот же id и та же версия,
        # изменённая (совместимая) → новая версия
        registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
        schema_id = registry.register_schema(self._subject, Schema(schema_str, "AVRO"))
        version = registry.get_latest_version(self._subject).version
        logger.info("schema registered: subject=%s id=%d version=%d", self._subject, schema_id, version)

        # Формат Confluent: 1 байт magic + 4 байта id схемы + Avro. По id ClickHouse достаёт схему из Registry
        self._serializer = AvroSerializer(registry, schema_str, conf={"auto.register.schemas": False})
        self._context = SerializationContext(config.KAFKA_TOPIC, MessageField.VALUE)

        self._producer = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            # запись подтверждена, когда её получили все in-sync реплики (их не меньше min.insync.replicas)
            "acks": "all",
            # повторы внутри клиента не создают дублей и не меняют порядок
            "enable.idempotence": True,
            # через столько клиент перестаёт пытаться сам и сообщает об ошибке — дальше решает retry ниже
            "message.timeout.ms": config.DELIVERY_TIMEOUT_MS,
        })

    def publish(self, record: dict) -> None:
        """Валидирует событие по схеме и публикует в Kafka; возвращается только после подтверждения записи."""
        try:
            validate(record, self._schema)
        except ValidationError as exc:
            raise InvalidEventError(str(exc)) from exc

        value = self._serializer(record, self._context)
        # ключ = user_id: все события пользователя в одной партиции, порядок внутри сессии сохраняется
        key = record["user_id"].encode()

        for attempt in range(1, config.PUBLISH_MAX_ATTEMPTS + 1):
            error, message = self._send(key, value)
            if error is None:
                logger.info(
                    "published event_id=%s event_type=%s timestamp=%s partition=%d offset=%d",
                    record["event_id"], record["event_type"], record["timestamp"].isoformat(),
                    message.partition(), message.offset(),
                )
                return
            if attempt == config.PUBLISH_MAX_ATTEMPTS:
                break
            delay_ms = config.PUBLISH_RETRY_BASE_DELAY_MS * 2 ** (attempt - 1)
            logger.warning(
                "publish failed event_id=%s attempt %d/%d: %s; retrying in %d ms",
                record["event_id"], attempt, config.PUBLISH_MAX_ATTEMPTS, error, delay_ms,
            )
            time.sleep(delay_ms / 1000)

        logger.error(
            "publish gave up event_id=%s after %d attempts: %s",
            record["event_id"], config.PUBLISH_MAX_ATTEMPTS, error,
        )
        raise PublishError(str(error))

    def _send(self, key: bytes, value: bytes):
        """Одна попытка: отправить и дождаться delivery report. Возвращает (ошибка | None, сообщение)."""
        delivered = threading.Event()
        report = {}

        def on_delivery(err, msg):
            report["err"], report["msg"] = err, msg
            delivered.set()

        try:
            self._producer.produce(config.KAFKA_TOPIC, key=key, value=value, on_delivery=on_delivery)
        except (BufferError, KafkaException) as exc:
            return exc, None
        # delivery report придёт всегда: либо успех, либо ошибка по message.timeout.ms
        while not delivered.is_set():
            self._producer.poll(0.05)
        return report["err"], report["msg"]

    def topic_is_empty(self) -> bool:
        """В топике ещё не было ни одного сообщения: верхняя граница смещений всех партиций равна нулю."""
        consumer = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS, "group.id": "movie-service-offsets"})
        try:
            partitions = consumer.list_topics(config.KAFKA_TOPIC, timeout=10).topics[config.KAFKA_TOPIC].partitions
            return all(
                consumer.get_watermark_offsets(TopicPartition(config.KAFKA_TOPIC, partition), timeout=10)[1] == 0
                for partition in partitions
            )
        finally:
            consumer.close()

    def close(self) -> None:
        self._producer.flush(5)
