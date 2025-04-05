import os
from airflow.utils import timezone
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="aqi_postgres_conn",
        profile_args={"schema": "public"},
    ),
)

dbt_dag = DbtDag(
    dag_id="aqi_dbt_dag",
    project_config=ProjectConfig(
        "/opt/airflow/AQI_project",
    ),
    profile_config=profile_config,
    schedule_interval="@daily",
    start_date=timezone.datetime(2025,4,5),
    catchup=False,
    default_args={"retries": 2},
    tags=["dpu", "aqi", "etl"],
)