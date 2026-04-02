from banking_learning_project import orchestration_module


def test_airflow_template():
    template = orchestration_module.airflow_dag_template("banking_test")
    assert "DAG" in template


def test_prefect_template():
    template = orchestration_module.prefect_flow_template("banking_test")
    assert "@flow" in template
