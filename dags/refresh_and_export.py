from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os, csv

DAG_ID = "telecom_refresh_and_export"
POSTGRES_CONN_ID = "telecom_db"

CREATE_MV_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_events_by_hour AS
SELECT date_trunc('hour', ts_utc) AS hour_utc,
       region,
       event_type,
       count(*) AS cnt,
       sum(COALESCE(data_mb,0)) AS mb_sum,
       sum(COALESCE(duration_seconds,0)) AS duration_sum
FROM telecom_events_raw
GROUP BY 1,2,3;
"""

REFRESH_MV_SQL = "REFRESH MATERIALIZED VIEW mv_events_by_hour;"

def export_24h_csv(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
    SELECT date_trunc('hour', ts_utc) AS hour_utc,
           region,
           event_type,
           count(*) AS cnt,
           sum(COALESCE(data_mb,0)) AS mb_sum,
           sum(COALESCE(duration_seconds,0)) AS duration_sum
    FROM telecom_events_raw
    WHERE ts_utc >= NOW() - interval '24 hour'
    GROUP BY 1,2,3
    ORDER BY 1 DESC, region, event_type;
    """
    rows = hook.get_records(sql)
    outdir = "/opt/airflow/logs/exports"
    os.makedirs(outdir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = f"{outdir}/agg_hourly_{ts}.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["hour_utc","region","event_type","cnt","mb_sum","duration_sum"])
        writer.writerows(rows)
    print(f"[export] wrote {path}, rows={len(rows)}")

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule_interval="*/5 * * * *", # каждые 5 минут
    catchup=False,
    max_active_runs=1,
    tags=["telecom","simple","topor"],
) as dag:

    ensure_mv = PostgresOperator(
        task_id="ensure_mv",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_MV_SQL,
    )

    refresh_mv = PostgresOperator(
        task_id="refresh_mv",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=REFRESH_MV_SQL,
    )

    export_csv = PythonOperator(
        task_id="export_last_24h_csv",
        python_callable=export_24h_csv,
        provide_context=True,
    )

    ensure_mv >> refresh_mv >> export_csv
