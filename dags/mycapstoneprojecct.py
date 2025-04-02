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
    
    # Push raw JSON to XCom
    context['ti'].xcom_push(key='raw_data', value=json.dumps(data))

# ---------- TASK 2: Transform ----------
def transform_aqi_data(**context):
    raw_data = json.loads(context['ti'].xcom_pull(task_ids='extract_aqi_data', key='raw_data'))

    aqi = raw_data['data']['current']['pollution']['aqius']
    ts = raw_data['data']['current']['pollution']['ts']
    fetched_at = datetime.utcnow().isoformat()

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
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS aqi_data (
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

# ---------- DAG Definition ----------
with DAG(
    dag_id="capstone_aqi_etl_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dpu", "aqi", "etl"],
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

    # Dependency: Extract >> Transform >> Load
    t1 >> t2 >> t3
