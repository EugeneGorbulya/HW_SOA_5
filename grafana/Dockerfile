# Grafana с плагином ClickHouse, datasource и дашбордом внутри образа (без bind-mount)
FROM grafana/grafana:12.3.0
RUN grafana cli plugins install grafana-clickhouse-datasource 4.23.5
COPY provisioning/ /etc/grafana/provisioning/
COPY dashboards/ /etc/grafana/dashboards/
