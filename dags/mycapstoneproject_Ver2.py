from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import psycopg2
import os
import json
from datetime import datetime

# ---------- TASK 1: Extract ----------
def extract_aqi_data(**context):
    api_key = os.getenv("AIRVISUAL_API_KEY")
    url = f"https://api.airvisual.com/v2/city?city=Bangkok&state=Bangkok&country=Thailand&key={api_key}"
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    context['ti'].xcom_push(key='raw_data', value=json.dumps(data))

# ---------- TASK 2: Transform ----------
def transform_aqi_data(**context):
    raw_data = json.loads(context['ti'].xcom_pull(task_ids='extract_aqi_data', key='raw_data'))

    aqi = raw_data['data']['current']['pollution']['aqius']
    ts = raw_data['data']['current']['pollution']['ts']
    fetched_at = datetime.utcnow().isoformat()

    # 🔍 เพิ่ม data quality checks
    if not isinstance(aqi, int) or not (0 <= aqi <= 500):
        raise ValueError(f"Invalid AQI value: {aqi}")
    if ts is None:
        raise ValueError("Missing timestamp in data")

    transformed = {
        "timestamp": ts,
        "aqi": aqi,
        "fetched_at": fetched_at
    }

    context['ti'].xcom_push(key='transformed_data', value=json.dumps(transformed))

# ---------- TASK 3: Load ----------
def load_to_postgres(**context):
    transformed = json.loads(context['ti'].xcom_pull(task_ids='transform_aqi_data', key='transformed_data'))

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS aqi_data_ver2 (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            aqi INTEGER,
            fetched_at TIMESTAMP
        );
    """)

    cur.execute("""
        INSERT INTO aqi_data (timestamp, aqi, fetched_at)
        VALUES (%s, %s, %s);
    """, (transformed['timestamp'], transformed['aqi'], transformed['fetched_at']))

    conn.commit()
    cur.close()
    conn.close()

# ---------- TASK 4: Summarize ----------
def summarize_aqi_data():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        port=5432
    )
    cur = conn.cursor()

    # 🔁 สร้างหรืออัปเดต summary table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aqi_summary_ver2 (
            day DATE PRIMARY KEY,
            max_aqi INTEGER,
            min_aqi INTEGER,
            avg_aqi FLOAT,
            readings INTEGER
        );
    """)

    # 🔁 ลบแล้วสร้างใหม่จากข้อมูลล่าสุด
    cur.execute("DELETE FROM aqi_summary;")

    cur.execute("""
        INSERT INTO aqi_summary (day, max_aqi, min_aqi, avg_aqi, readings)
        SELECT 
            DATE(timestamp) AS day,
            MAX(aqi),
            MIN(aqi),
            AVG(aqi),
            COUNT(*)
        FROM aqi_data
        GROUP BY day
        ORDER BY day;
    """)

    conn.commit()
    cur.close()
    conn.close()

# ---------- TASK 5: Business Insights ----------
def generate_business_insights():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        port=5432
    )
    cur = conn.cursor()

    # ✅ สร้าง schema ถ้ายังไม่มี
    cur.execute("CREATE SCHEMA IF NOT EXISTS dbt_kantinan;")

    # ✅ ใช้ schema dbt_kantinan สร้างตาราง insights
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dbt_kantinan.aqi_insights (
            id SERIAL PRIMARY KEY,
            metric TEXT,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # ✅ เคลียร์ข้อมูลเดิม (หรือจะใช้ INSERT ON CONFLICT ก็ได้)
    cur.execute("DELETE FROM dbt_kantinan.aqi_insights;")

    # ✅ Insert metric answers
    insights_queries = [
        ("highest_aqi_this_week", """
            SELECT MAX(aqi) FROM aqi_data
            WHERE DATE_TRUNC('week', timestamp) = DATE_TRUNC('week', CURRENT_DATE);
        """),
        ("lowest_aqi_last_3_months", """
            SELECT MIN(aqi) FROM aqi_data
            WHERE timestamp >= CURRENT_DATE - INTERVAL '3 months';
        """),
        ("avg_aqi_this_week", """
            SELECT AVG(aqi) FROM aqi_data
            WHERE DATE_TRUNC('week', timestamp) = DATE_TRUNC('week', CURRENT_DATE);
        """),
        ("worst_day_last_month", """
            SELECT TO_CHAR(DATE(timestamp), 'YYYY-MM-DD') || ' (AQI: ' || MAX(aqi) || ')' FROM aqi_data
            WHERE timestamp >= CURRENT_DATE - INTERVAL '1 month'
            GROUP BY DATE(timestamp)
            ORDER BY MAX(aqi) DESC
            LIMIT 1;
        """),
        ("unhealthy_days_last_30d", """
            SELECT COUNT(DISTINCT DATE(timestamp)) FROM aqi_data
            WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
            AND aqi > 100;
        """)
    ]

    for metric, query in insights_queries:
        cur.execute(query)
        result = cur.fetchone()
        value = str(result[0]) if result else 'N/A'
        cur.execute("""
            INSERT INTO dbt_kantinan.aqi_insights (metric, value)
            VALUES (%s, %s);
        """, (metric, value))

    conn.commit()
    cur.close()
    conn.close()

# ---------- DAG ----------
with DAG(
    dag_id="capstone_datapipeline_ver2",
    start_date=days_ago(1),
    schedule_interval="0 */12 * * *",
    catchup=False,
    tags=["dpu"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_aqi_data",
        python_callable=extract_aqi_data,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="transform_aqi_data",
        python_callable=transform_aqi_data,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="summarize_aqi_data",
        python_callable=summarize_aqi_data,
    )
    t5 = PythonOperator(
        task_id="generate_business_insights",
        python_callable=generate_business_insights,
    )

    t1 >> t2 >> t3 >> t4 >> t5
