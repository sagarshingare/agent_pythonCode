"""Orchestration module covering Airflow and Prefect patterns."""

from typing import Callable


def airflow_dag_template(dag_id: str):
    """Return Airflow DAG code template as string."""
    return f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def dummy_task():
    print('dummy task')

with DAG(dag_id='{dag_id}', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    run = PythonOperator(task_id='run', python_callable=dummy_task)
    run
"""


def prefect_flow_template(flow_name: str):
    """Return Prefect flow code template as string."""
    return f"""
from prefect import flow, task

@task
def dummy_task():
    print('dummy task')

@flow(name='{flow_name}')
def main_flow():
    dummy_task()

if __name__ == '__main__':
    main_flow()
"""


def example():
    print("--- ORCHESTRATION MODULE ---")
    print("Airflow DAG template:\n", airflow_dag_template("banking_pipeline"))
    print("Prefect flow template:\n", prefect_flow_template("banking_flow"))
