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

task_m_comptime_build_message_counters = SparkSubmitOperator(
    task_id="task_m_comptime_build_message_counters",
    application="pyspark/job.py",
    conf={"spark.app.name": "m_COMPTIME_Build_Message_Counters"},
    dag=dag,
)

task_m_comptime_load_comp_time_daily_tbl = SparkSubmitOperator(
    task_id="task_m_comptime_load_comp_time_daily_tbl",
    application="pyspark/job.py",
    conf={"spark.app.name": "m_COMPTIME_Load_COMP_TIME_DAILY_TBL"},
    dag=dag,
)

task_m_comptime_build_message_counters >> task_m_comptime_load_comp_time_daily_tbl

task_m_comptime_current_pay_period = SparkSubmitOperator(
    task_id="task_m_comptime_current_pay_period",
    application="pyspark/job.py",
    conf={"spark.app.name": "m_COMPTIME_Current_Pay_Period"},
    dag=dag,
)

task_m_comptime_load_comp_time_daily_tbl >> task_m_comptime_current_pay_period

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