.PHONY: up down ps logs test topic schema clickhouse psql

up:
	docker compose up --build -d

down:
	docker compose down -v

ps:
	docker compose ps -a

logs:
	docker compose logs -f --no-log-prefix producer aggregation-service

# п.4: интеграционные тесты. Если система не поднята — поднимет нужные сервисы сама
test:
	docker compose build tests
	docker compose run --rm tests

# п.1, п.8: партиции, реплики и ISR топика
topic:
	docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic movie-events

# п.1: версии схемы в Schema Registry
schema:
	curl -s http://localhost:8081/subjects/movie-events-value/versions/latest

clickhouse:
	docker compose exec clickhouse clickhouse-client --user cinema --password cinema --database cinema

psql:
	docker compose exec postgres psql -U analytics -d analytics
