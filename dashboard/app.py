import os
import time
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "rt_db")
DB_USER = os.getenv("DB_USER", "rt_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rt_password")

@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, cursor_factory=RealDictCursor
    )

def q(sql, params=None) -> pd.DataFrame:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or {})
        rows = cur.fetchall()
    return pd.DataFrame(rows)

st.set_page_config(page_title="Telecom Dashboard", layout="wide")
st.title("Telecom dashboards")

st.experimental_set_query_params(ts=int(time.time()))
time.sleep(0.1)

col_a, col_b, col_c, col_d = st.columns(4)

# KPI: строки за 1ч, за 24ч, суммарный MB за 24ч, уникальные MSISDN за 24ч
kpi_1h = q("""
SELECT count(*) AS rows_1h
FROM telecom_events_raw
WHERE ts_utc >= NOW() - interval '1 hour'
""")
kpi_24h = q("""
SELECT count(*) AS rows_24h,
       sum(COALESCE(data_mb,0)) AS mb_24h,
       count(DISTINCT msisdn) AS uniq_msisdn_24h
FROM telecom_events_raw
WHERE ts_utc >= NOW() - interval '24 hour'
""")

col_a.metric("Событий (1ч)", int(kpi_1h.iloc[0]["rows_1h"]))
col_b.metric("Событий (24ч)", int(kpi_24h.iloc[0]["rows_24h"]))
col_c.metric("Трафик, MB (24ч)", round(float(kpi_24h.iloc[0]["mb_24h"] or 0), 2))
col_d.metric("Уникальных MSISDN (24ч)", int(kpi_24h.iloc[0]["uniq_msisdn_24h"]))

st.divider()

# График 1: события по типам (последний час)
df_types = q("""
SELECT event_type, count(*) AS cnt
FROM telecom_events_raw
WHERE ts_utc >= NOW() - interval '1 hour'
GROUP BY event_type
ORDER BY cnt DESC
""")
st.subheader("События по типам (1ч)")
st.bar_chart(df_types.set_index("event_type"))

st.divider()

# График 2: трафик по регионам (24ч)
df_mb_region = q("""
SELECT region, sum(COALESCE(data_mb,0)) AS mb_sum
FROM telecom_events_raw
WHERE ts_utc >= NOW() - interval '24 hour'
GROUP BY region
ORDER BY mb_sum DESC
""")
st.subheader("Суммарный трафик, MB (24ч), по регионам")
st.bar_chart(df_mb_region.set_index("region"))

st.divider()

# График 3: топ вышек (24ч)
df_towers = q("""
SELECT cell_tower_id, count(*) AS cnt
FROM telecom_events_raw
WHERE ts_utc >= NOW() - interval '24 hour'
GROUP BY cell_tower_id
ORDER BY cnt DESC
LIMIT 15
""")
st.subheader("Топ-15 вышек по числу событий (24ч)")
st.bar_chart(df_towers.set_index("cell_tower_id"))

st.caption("Источник данных: Postgres → public.telecom_events_raw")
