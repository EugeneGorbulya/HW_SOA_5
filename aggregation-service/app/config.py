import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "cinema")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://analytics:analytics@localhost:15440/analytics")
# Запись в PostgreSQL: всего попыток и база экспоненциальной паузы
POSTGRES_MAX_ATTEMPTS = int(os.getenv("POSTGRES_MAX_ATTEMPTS", "3"))
POSTGRES_RETRY_BASE_DELAY_MS = int(os.getenv("POSTGRES_RETRY_BASE_DELAY_MS", "500"))

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")

# п.5: как часто запускать пересчёт по расписанию
AGGREGATION_INTERVAL_SECONDS = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "60"))
# Плановый цикл пересчитывает сегодня и столько предыдущих дней: туда попадают опоздавшие события
AGGREGATION_LOOKBACK_DAYS = int(os.getenv("AGGREGATION_LOOKBACK_DAYS", "8"))
# п.7: когда выгружать в S3 метрики за вчера (cron, UTC)
EXPORT_CRON = os.getenv("EXPORT_CRON", "5 0 * * *")
