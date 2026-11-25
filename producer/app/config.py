import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "/app/schemas/movie_event.avsc")

# п.2: retry публикации — всего попыток (вместе с первой) и база экспоненциальной паузы: 200 мс, 400 мс, ...
PUBLISH_MAX_ATTEMPTS = int(os.getenv("PUBLISH_MAX_ATTEMPTS", "3"))
PUBLISH_RETRY_BASE_DELAY_MS = int(os.getenv("PUBLISH_RETRY_BASE_DELAY_MS", "200"))
# Сколько ждать подтверждения записи от брокеров в одной попытке
DELIVERY_TIMEOUT_MS = int(os.getenv("DELIVERY_TIMEOUT_MS", "5000"))

# Генератор синтетических событий
GENERATOR_ENABLED = os.getenv("GENERATOR_ENABLED", "true").lower() == "true"
# История за сколько прошлых дней создаётся при старте (0 — без истории); нужна, чтобы retention было из чего считать
GENERATOR_BACKFILL_DAYS = int(os.getenv("GENERATOR_BACKFILL_DAYS", "8"))
GENERATOR_NEW_USERS_PER_DAY = int(os.getenv("GENERATOR_NEW_USERS_PER_DAY", "30"))
GENERATOR_SESSIONS_PER_MINUTE = float(os.getenv("GENERATOR_SESSIONS_PER_MINUTE", "20"))
