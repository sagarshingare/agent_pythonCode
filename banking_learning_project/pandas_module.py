"""Pandas banking data manipulation examples.

Uses pandas API methods available in pandas 1.x and 2.x.
"""

import pandas as pd


def load_transaction_dataframe(transactions: list) -> pd.DataFrame:
    """Convert transaction list to DataFrame and validate required columns."""
    df = pd.DataFrame(transactions)
    expected = ["txn_id", "account_id", "amount", "type", "date"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df


def normalize_transaction_types(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure transaction types are lowercase and valid (credit/debit)."""
    df2 = df.copy()
    df2["type"] = df2["type"].str.lower().replace({"cr": "credit", "dr": "debit"})
    if not df2["type"].isin(["credit", "debit"]).all():
        raise ValueError("Invalid transaction type found")
    return df2


def add_signed_amount(df: pd.DataFrame) -> pd.DataFrame:
    """Add signed_amount for debits (-) and credits (+)."""
    df2 = df.copy()
    df2["signed_amount"] = df2.apply(
        lambda row: row["amount"] if row["type"] == "credit" else -row["amount"], axis=1
    )
    return df2


def compute_monthly_balance(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-account monthly balance totals."""
    df2 = df.copy()
    df2["date"] = pd.to_datetime(df2["date"])
    df2["year_month"] = df2["date"].dt.to_period("M")
    return (
        df2.groupby(["account_id", "year_month"])["signed_amount"]
        .sum()
        .reset_index(name="net_balance")
    )


def detect_unusual_transactions(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """Flag high-value transactions using absolute amount."""
    return df[df["amount"].abs() > threshold]


def rolling_avg_balance(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """Compute rolling average net balance per account."""
    df2 = df.copy()
    df2["date"] = pd.to_datetime(df2["date"])
    df2 = add_signed_amount(df2)
    df2 = df2.sort_values(["account_id", "date"])
    return df2.groupby("account_id")["signed_amount"].rolling(window, min_periods=1).mean().reset_index(name="rolling_avg")


def pivot_monthly_by_type(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot net amounts by transaction type per account and month."""
    df2 = add_signed_amount(normalize_transaction_types(df))
    df2["date"] = pd.to_datetime(df2["date"])
    df2["year_month"] = df2["date"].dt.to_period("M")
    return (
        df2.pivot_table(
            index=["account_id", "year_month"],
            columns="type",
            values="signed_amount",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )


def example() -> None:
    """Banking pandas example flows."""
    transactions = [
        {"txn_id": "T001", "account_id": "A001", "amount": 2500.0, "type": "credit", "date": "2025-12-01"},
        {"txn_id": "T002", "account_id": "A001", "amount": 150.0, "type": "debit", "date": "2025-12-05"},
        {"txn_id": "T003", "account_id": "A002", "amount": 10000.0, "type": "credit", "date": "2025-12-07"},
    ]
    df = load_transaction_dataframe(transactions)
    print("--- PANDAS MODULE EXAMPLE ---")
    df2 = normalize_transaction_types(df)
    df2 = add_signed_amount(df2)
    print(compute_monthly_balance(df2))
    print(detect_unusual_transactions(df2, threshold=2000.0))
    print(rolling_avg_balance(df2, window=2))
    print(pivot_monthly_by_type(df2))
