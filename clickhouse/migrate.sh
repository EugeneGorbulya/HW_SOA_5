#!/bin/sh
# Применяет миграции по порядку имён. Все запросы — CREATE ... IF NOT EXISTS, поэтому повторный запуск безопасен.
set -e
for file in /migrations/*.sql; do
    echo "applying $(basename "$file")"
    clickhouse-client --host "$CLICKHOUSE_HOST" --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
        --queries-file "$file"
done
echo "ClickHouse migrations applied"
