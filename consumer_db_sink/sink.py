import os, json, time, signal, sys
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
import psycopg2.extras as extras

KAFKA_BOOTSTRAP = os.getenv("DB_SINK_KAFKA_BOOTSTRAP", "kafka:9093")
TOPIC = os.getenv("DB_SINK_KAFKA_TOPIC", "telecom_events")
GROUP_ID = os.getenv("DB_SINK_GROUP_ID", "pg_sink_group")

BATCH_SIZE = int(os.getenv("DB_SINK_BATCH_SIZE", "200"))
FLUSH_SECONDS = int(os.getenv("DB_SINK_FLUSH_SECONDS", "5"))

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "rt_db")
DB_USER = os.getenv("DB_USER", "rt_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rt_password")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS telecom_events_raw (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID,
    msisdn TEXT,
    event_type TEXT,
    event_subtype TEXT,
    duration_seconds INT,
    data_mb NUMERIC,
    amount NUMERIC,
    region TEXT,
    cell_tower_id INT,
    ts_utc TIMESTAMPTZ,
    raw_json JSONB
);
CREATE INDEX IF NOT EXISTS idx_ter_ts ON telecom_events_raw (ts_utc);
CREATE INDEX IF NOT EXISTS idx_ter_event_type ON telecom_events_raw (event_type);
CREATE INDEX IF NOT EXISTS idx_ter_region ON telecom_events_raw (region);
"""

INSERT_SQL = """
INSERT INTO telecom_events_raw (
    event_id, msisdn, event_type, event_subtype,
    duration_seconds, data_mb, amount, region,
    cell_tower_id, ts_utc, raw_json
) VALUES %s
"""

def pg_connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_SQL)
    conn.commit()

def to_row(doc):
    # doc — это dict из продьюсера
    # timestamp в ISO8601 -> timestamptz
    ts = doc.get("timestamp")
    ts_dt = None
    if ts:
        try:
            ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            ts_dt = None
    return (
        doc.get("event_id"),
        doc.get("msisdn"),
        doc.get("event_type"),
        doc.get("event_subtype"),
        doc.get("duration_seconds"),
        doc.get("data_mb"),
        doc.get("amount"),
        doc.get("region"),
        doc.get("cell_tower_id"),
        ts_dt,
        json.dumps(doc),
    )

def main():
    print(f"[db-sink] bootstrap={KAFKA_BOOTSTRAP} topic={TOPIC} group={GROUP_ID}")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    conn = pg_connect()
    ensure_schema(conn)
    cur = conn.cursor()

    buf = []
    last_flush = time.time()

    def flush():
        nonlocal buf, last_flush
        if not buf:
            return
        rows = [to_row(d) for d in buf]
        extras.execute_values(cur, INSERT_SQL, rows, page_size=min(len(rows), 1000))
        conn.commit()
        consumer.commit()
        print(f"[db-sink] inserted {len(rows)} rows")
        buf = []
        last_flush = time.time()

    def handle_sig(sig, frame):
        print("[db-sink] shutting down...")
        flush()
        cur.close()
        conn.close()
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    for msg in consumer:
        buf.append(msg.value)
        if len(buf) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_SECONDS:
            flush()

if __name__ == "__main__":
    main()
