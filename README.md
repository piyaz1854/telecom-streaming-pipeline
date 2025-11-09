# telecom-streaming-pipeline
Real-time telecom events with Kafka &amp; Spark, batch via Airflow, metrics in PostgreSQL

# Telecom Streaming Pipeline — Часть 1. Базовая инфраструктура

Базовый стенд для локальной разработки и экспериментов:
- **Zookeeper** (Confluent 7.7)
- **Kafka** (Confluent 7.7) — внешний `localhost:9092`, внутренний `kafka:9093`
- **Postgres** (16) — `localhost:5432`
- **Airflow** (2.9.2) — Web UI `http://localhost:8080`, `LocalExecutor`
- Персистентные **volumes**, общая **bridge-сеть** `rt_net`
- Все конфиги и секреты — только в `.env`

---

## 0) Предусловия

- Docker Desktop или Docker Engine + Docker Compose v2  
- (опционально) `jq` для красивого JSON

Клонирование репозитория:
```bash
git clone <your-repo>
cd telecom-streaming-pipeline
```

## 1) Создание .env
```
# ==== Images ====
ZOOKEEPER_IMAGE=confluentinc/cp-zookeeper:7.7.0
KAFKA_IMAGE=confluentinc/cp-kafka:7.7.0
POSTGRES_IMAGE=postgres:16
AIRFLOW_IMAGE=apache/airflow:2.9.2

# ==== Network ====
NETWORK_NAME=rt_net

# ==== Zookeeper / Kafka ====
ZOO_PORT=2181
KAFKA_PORT=9092
KAFKA_INTERNAL_PORT=9093

# ==== Postgres ====
POSTGRES_USER=rt_user
POSTGRES_PASSWORD=rt_password
POSTGRES_DB=rt_db
POSTGRES_PORT=5432

# ==== Airflow ====
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=<вставь_44-символьный_base64_ключ>
AIRFLOW_EXECUTOR=LocalExecutor
AIRFLOW_LOAD_EXAMPLES=False
AIRFLOW_PORT=8080

# ==== Airflow Admin ====
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
```
Сгенерировать Fernet key (без сторонних пакетов):
```
python3 -c "import os, base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```
## 2) Запуск стенда
```
docker compose --env-file .env up -d
```
Проверка статусов
```
docker compose ps
```
## 3) Airflow — проверка здоровья и вход
Проверка /health с хоста:
```
curl -fsS http://localhost:8080/health | jq .
```

## 4) Kafka — смоук-тест
Внутри контейнеров подключаемся к INTERNAL listener kafka:9093.
С хоста — к EXTERNAL listener localhost:9092.

3.1 Создать топик (через EXTERNAL)
```
docker compose exec kafka bash -lc \
'kafka-topics --bootstrap-server localhost:9092 \
  --create --topic rt_smoke --replication-factor 1 --partitions 1'
```
3.2 Отправить сообщения (через INTERNAL)
```
docker compose exec -T kafka bash -lc \
'printf "hello\nworld\n" | kafka-console-producer --broker-list kafka:9093 --topic rt_smoke'
```
3.3 Прочитать сообщения с начала (через INTERNAL)
```
docker compose exec kafka bash -lc \
'kafka-console-consumer --bootstrap-server kafka:9093 \
  --topic rt_smoke --from-beginning --timeout-ms 5000 --max-messages 999'
```
Ожидаемый вывод:
```
hello
world
```
3.4 Проверить lag группы (опционально)
```
# читаем в группе, чтобы зафиксировать оффсеты
docker compose exec kafka bash -lc \
'kafka-console-consumer --bootstrap-server kafka:9093 \
  --topic rt_smoke --group rt_group --from-beginning --max-messages 999 --timeout-ms 5000'

# lag должен быть 0
docker compose exec kafka bash -lc \
'kafka-consumer-groups --bootstrap-server kafka:9093 --group rt_group --describe'
```
## 5) Что за что отвечает

| Компонент         | Адрес                   | Назначение                     |
| ----------------- | ----------------------- | ------------------------------ |
| Zookeeper         | `localhost:2181`        | координация Kafka              |
| Kafka (EXTERNAL)  | `localhost:9092`        | доступ с хоста                 |
| Kafka (INTERNAL)  | `kafka:9093`            | межконтейнерный трафик         |
| Postgres          | `localhost:5432`        | Airflow метастор + ваши данные |
| Airflow Web UI    | `http://localhost:8080` | оркестрация DAG’ов             |
| Airflow Scheduler | в сети `rt_net`         | планировщик                    |
| Airflow Triggerer | в сети `rt_net`         | deferred-операторы             |

## 6) Остановка и очистка
Остановить (сохранить данные):
```
docker compose down
```
Полная очистка (удалить тома):
```
docker compose down -v
```

## Архитектура

                        (Real-time Events Generator)
                                      │
                                      ▼
                             Kafka Topic `telecom_events`
                                      │
                                      ▼
                          Python Consumer (db-sink-consumer)
                                      │
                                      ▼
                                 PostgreSQL (raw data)
                                      │
                                      ▼
                     Airflow DAG: Refresh + Aggregation + Export
                                      │
                                      ▼
                     Materialized Views + CSV (*.csv per batch)
                                      │
                                      ▼
                               Streamlit Dashboard

## Запуск генератора
Симулятор создаёт случайные события телеком-сети (звонки, SMS, интернет-сессии и т.д.) и пушит их в Kafka
```
docker compose up -d rt-event-producer
```

Посмотреть логи:
```
docker logs -f rt-event-producer
```

## Consumer который сохраняет события в Postgres:
```
docker compose up -d db-sink-consumer
```

Быстрая проверка что все работает:
```
docker exec -it postgres psql -U rt_user -d rt_db -c \
"SELECT * FROM telecom_events_raw ORDER BY ts_utc DESC LIMIT 10;"
```

## Ручной запуск DAG:
```
docker exec -it airflow-scheduler bash -lc \
"airflow dags trigger telecom_refresh_and_export"
```

CSV будут появляться в контейнере Airflow:
```
/opt/airflow/logs/exports/agg_hourly_*.csv
```

Проверка файлов:
```
docker exec -it airflow-webserver bash -lc \
'ls -lh /opt/airflow/logs/exports'
```

## Streamlit
Запуск:
```
docker compose up -d dashboard
```
И будет доступно на:
```
http://localhost:8501
```
