"""Pandas ETL pipelines for Data Engineering Architect project."""

import pandas as pd
from typing import List, Dict


def load_transactions(transactions: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(transactions)
    df.date = pd.to_datetime(df.date)
    return df


def summarize_balance(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby('account_id').amount.sum().reset_index(name='balance')


def example() -> None:
    print('--- DATA ENG ARCH PANDAS ETL ---')
    sample = [{'account_id': 'A1', 'amount': 100, 'date': '2025-01-01'}, {'account_id': 'A1', 'amount': -20, 'date': '2025-01-02'}]
    df = load_transactions(sample)
    print(summarize_balance(df))
