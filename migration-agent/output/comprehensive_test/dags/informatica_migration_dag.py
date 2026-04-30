"""
Auto-generated Apache Airflow DAG for Informatica Migration
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data_engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "informatica_migration_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["informatica", "migration", "pyspark"],
)

# Tasks

task_m_comprehensive_poc = SparkSubmitOperator(
    task_id="task_m_comprehensive_poc",
    application="pyspark/job.py",
    conf={"spark.app.name": "m_comprehensive_poc"},
    dag=dag,
)

# SLA and Alerts
task_success_alert = PythonOperator(
    task_id="success_alert",
    python_callable=lambda: print("Migration job completed successfully"),
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

task_failure_alert = PythonOperator(
    task_id="failure_alert",
    python_callable=lambda: print("Migration job failed"),
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
)