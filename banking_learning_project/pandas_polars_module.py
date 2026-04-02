"""Combined Pandas + Polars banking data processing examples."""

from typing import List, Dict

import pandas as pd

try:
    import polars as pl
except ImportError:
    pl = None


def transform_transactions_pandas(transactions: List[Dict]) -> pd.DataFrame:
    """Pandas transformation pipeline (must master data science)."""
    df = pd.DataFrame(transactions)
    df["date"] = pd.to_datetime(df["date"])
    df["signed_amount"] = df.apply(lambda r: r["amount"] if r["type"] == "credit" else -r["amount"], axis=1)
    return df


def transform_transactions_polars(transactions: List[Dict]) -> "pl.DataFrame":
    """Polars pipeline (fast data processing, can be used if polars installed)."""
    if pl is None:
        raise ImportError("polars not installed. pip install polars")
    df = pl.DataFrame(transactions)
    return (
        df.with_column(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("date"))
          .with_column(pl.when(pl.col("type") == "credit").then(pl.col("amount")).otherwise(-pl.col("amount")).alias("signed_amount"))
    )


def example():
    transactions = [
        {"txn_id": "T1", "account_id": "A1", "amount": 1200.0, "type": "credit", "date": "2025-01-10"},
        {"txn_id": "T2", "account_id": "A1", "amount": 300.0, "type": "debit", "date": "2025-01-11"},
    ]
    print("--- PANDAS/POLARS MODULE ---")
    pandas_df = transform_transactions_pandas(transactions)
    print(pandas_df)
    if pl is not None:
        polars_df = transform_transactions_polars(transactions)
        print(polars_df)
    else:
        print("polars not installed: skip polars example")
