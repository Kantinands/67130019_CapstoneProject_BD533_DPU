from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import requests
import json
import os
from datetime import datetime

# Extract Data
def extract_aqi_data(**context):
    api_key = os.getenv("AIRVISUAL_API_KEY")
    url = f"https://api.airvisual.com/v2/city?city=Bangkok&state=Bangkok&country=Thailand&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    context['ti'].xcom_push(key='raw_data', value=json.dumps(data))

# Transform
def transform_aqi_data(**context):
    raw_data = json.loads(context['ti'].xcom_pull(task_ids='extract_aqi_data', key='raw_data'))
    aqi = raw_data['data']['current']['pollution']['aqius']
    ts = raw_data['data']['current']['pollution']['ts']
    retrieve_information_time = datetime.utcnow().isoformat()

    transformed = {
        "timestamp": ts,
        "aqi": aqi,
        "retrieve_information_time": retrieve_information_time
    }
    context['ti'].xcom_push(key='transformed_data', value=json.dumps(transformed))

# Data Quality Check
def check_data_quality(**context):
    data = json.loads(context['ti'].xcom_pull(task_ids='transform_aqi_data', key='transformed_data'))
    required_fields = ['timestamp', 'aqi', 'retrieve_information_time']
    for field in required_fields:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing required field: {field}")
    if not isinstance(data['aqi'], int) or not (0 <= data['aqi'] <= 500):
        raise ValueError(f"Invalid AQI value: {data['aqi']}")
    print("Data quality passed.")

# Load using PostgresHook
def load_to_postgres(**context):
    transformed = json.loads(context['ti'].xcom_pull(task_ids='transform_aqi_data', key='transformed_data'))
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    hook.run("""
        CREATE TABLE IF NOT EXISTS aqi_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            aqi INTEGER NOT NULL,
            retrieve_information_time TIMESTAMP NOT NULL
        );
    """)
    
    hook.run("""
        INSERT INTO aqi_data (timestamp, aqi, retrieve_information_time)
        VALUES (%s, %s, %s);
    """, parameters=(transformed['timestamp'], transformed['aqi'], transformed['retrieve_information_time']))

# Summarize using PostgresHook
def summarize_aqi_data():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        CREATE TABLE IF NOT EXISTS aqi_summary (
            day DATE PRIMARY KEY,
            max_aqi INTEGER NOT NULL,
            min_aqi INTEGER NOT NULL,
            avg_aqi FLOAT NOT NULL,
            readings INTEGER NOT NULL
        );
    """)
    hook.run("DELETE FROM aqi_summary;")
    hook.run("""
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

# Business Insight using PostgresHook
def generate_business_insights():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS business_insight;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS business_insight.aqi_insights (
            id SERIAL PRIMARY KEY,
            metric TEXT,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("DELETE FROM business_insight.aqi_insights;")

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
            INSERT INTO business_insight.aqi_insights (metric, value)
            VALUES (%s, %s);
        """, (metric, value))

    conn.commit()
    cur.close()

# ---------- DAG ----------
with DAG(
    dag_id="capstone_datapipeline_ver2",
    start_date=days_ago(1),
    schedule_interval="0 */12 * * *",
    catchup=False,
    tags=["dpu","CapstoneProject"],
) as dag:

    t1 = PythonOperator(task_id="extract_aqi_data", python_callable=extract_aqi_data)
    t2 = PythonOperator(task_id="transform_aqi_data", python_callable=transform_aqi_data)
    t3 = PythonOperator(task_id="check_data_quality", python_callable=check_data_quality)
    t4 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t5 = PythonOperator(task_id="summarize_aqi_data", python_callable=summarize_aqi_data)
    t6 = PythonOperator(task_id="generate_business_insights", python_callable=generate_business_insights)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
