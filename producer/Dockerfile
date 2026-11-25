FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Avro-схема лежит в ../schemas; контекст `schemas` подключается в docker-compose.yml через additional_contexts
COPY --from=schemas movie_event.avsc schemas/
COPY app/ app/
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
USER nobody
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
