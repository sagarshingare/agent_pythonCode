import pytest

try:
    from banking_learning_project import pyspark_module
except ImportError:
    pyspark_module = None


@pytest.mark.skipif(pyspark_module is None, reason="pyspark module not available")
def test_get_spark_session_stop():
    spark = pyspark_module.get_spark_session("test_app", "local[1]")
    assert spark is not None
    pyspark_module.stop_spark_session(spark)


@pytest.mark.skipif(pyspark_module is None, reason="pyspark module not available")
def test_create_dataframe_add_signed_balance_and_net_balance():
    spark = pyspark_module.get_spark_session("test_app", "local[1]")
    try:
        txns = [
            {"txn_id": "P001", "account_id": "A001", "amount": 1000.0, "txn_type": "credit", "date": "2025-12-01"},
            {"txn_id": "P002", "account_id": "A001", "amount": 300.0, "txn_type": "debit", "date": "2025-12-02"},
        ]
        df = pyspark_module.create_dataframe(spark, txns)
        df2 = pyspark_module.add_signed_amount(df)
        balance_df = pyspark_module.compute_net_balance_by_account(df2)

        rows = {row['account_id']: row['net_balance'] for row in balance_df.collect()}
        assert rows['A001'] == pytest.approx(700.0)

    finally:
        pyspark_module.stop_spark_session(spark)


@pytest.mark.skipif(pyspark_module is None, reason="pyspark module not available")
def test_ml_pipeline_linear_regression():
    spark = pyspark_module.get_spark_session("test_app", "local[1]")
    try:
        ml_data = [
            {"feature1": 2.0, "feature2": 5.0, "label": 7.0},
            {"feature1": 3.0, "feature2": 6.0, "label": 9.0},
            {"feature1": 4.0, "feature2": 4.0, "label": 8.0},
        ]
        df = pyspark_module.create_dataframe(spark, ml_data)
        model = pyspark_module.ml_pipeline_linear_regression(df, ["feature1", "feature2"], label_col="label")
        predictions = pyspark_module.spark_ml_predict(model, df)
        assert "prediction" in predictions.columns
        assert predictions.count() == 3

    finally:
        pyspark_module.stop_spark_session(spark)
