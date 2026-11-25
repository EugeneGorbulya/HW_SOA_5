# Миграции ClickHouse внутри образа (без bind-mount); clickhouse-client берём из образа сервера
FROM clickhouse/clickhouse-server:25.8
COPY migrations/ /migrations/
COPY --chmod=755 migrate.sh /migrate.sh
ENTRYPOINT ["/migrate.sh"]
