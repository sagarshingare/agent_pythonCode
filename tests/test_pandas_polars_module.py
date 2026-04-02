from banking_learning_project import pandas_polars_module


def test_transform_transactions_pandas():
    txns = [{"txn_id": "T1", "account_id": "A1", "amount": 100.0, "type": "credit", "date": "2025-02-01"}]
    df = pandas_polars_module.transform_transactions_pandas(txns)
    assert "signed_amount" in df.columns


def test_transform_transactions_polars_skip_if_unavailable():
    if pandas_polars_module.pl is None:
        import pytest
        pytest.skip("polars not installed")
    txns = [{"txn_id": "T1", "account_id": "A1", "amount": 100.0, "type": "credit", "date": "2025-02-01"}]
    df = pandas_polars_module.transform_transactions_polars(txns)
    assert df.select("signed_amount").to_series()[0] == 100.0
