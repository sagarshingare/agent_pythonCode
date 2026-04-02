import pandas as pd
from banking_learning_project import pandas_module


def test_load_transaction_dataframe():
    txns = [{"txn_id": "T1", "account_id": "A1", "amount": 100, "type": "credit", "date": "2025-01-01"}]
    df = pandas_module.load_transaction_dataframe(txns)
    assert isinstance(df, pd.DataFrame)


def test_add_signed_amount_and_monthly_balance():
    txns = [
        {"txn_id": "T1", "account_id": "A1", "amount": 100, "type": "credit", "date": "2025-01-01"},
        {"txn_id": "T2", "account_id": "A1", "amount": 50, "type": "debit", "date": "2025-01-05"},
    ]
    df = pandas_module.load_transaction_dataframe(txns)
    df2 = pandas_module.normalize_transaction_types(df)
    df2 = pandas_module.add_signed_amount(df2)
    monthly = pandas_module.compute_monthly_balance(df2)
    assert monthly.loc[0, "net_balance"] == 50


def test_pivot_monthly_by_type():
    txns = [
        {"txn_id": "T1", "account_id": "A1", "amount": 100, "type": "credit", "date": "2025-01-01"},
        {"txn_id": "T2", "account_id": "A1", "amount": 50, "type": "debit", "date": "2025-01-05"},
    ]
    df = pandas_module.load_transaction_dataframe(txns)
    res = pandas_module.pivot_monthly_by_type(df)
    assert "credit" in res.columns and "debit" in res.columns
