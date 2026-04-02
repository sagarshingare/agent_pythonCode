"""Basic Python operations with banking domain examples.

This module is designed to illustrate core Python functions, collections, data validation,
and business logic for banking features.
"""

from typing import Dict, List, Optional, Union


def calculate_interest(principal: float, rate: float, years: int) -> float:
    """Compute simple interest for a bank deposit.

    Example: principal=10000, rate=3.5, years=2 => 700.0
    """
    if principal < 0 or rate < 0 or years < 0:
        raise ValueError("Principal, rate and years must be non-negative")
    return principal * rate * years / 100.0


def calculate_compound_interest(principal: float, rate: float, times_per_year: int, years: float) -> float:
    """Compute compound interest (Python math library style)."""
    if principal < 0 or rate < 0 or times_per_year <= 0 or years < 0:
        raise ValueError("Parameters must be non-negative, times_per_year > 0")
    return principal * ((1 + rate / times_per_year) ** (times_per_year * years))


def summarize_account(account: Dict[str, Union[str, float]]) -> str:
    """Generate an account summary string."""
    return (f"Account {account['account_id']}: balance={account['balance']:.2f}, "
            f"status={account.get('status', 'active')}, "
            f"type={account.get('type', 'checking')}")


def filter_accounts_by_balance(accounts: List[Dict[str, Union[str, float]]], min_balance: float) -> List[Dict[str, Union[str, float]]]:
    """Return accounts with balance at or above threshold."""
    return [acct for acct in accounts if acct.get('balance', 0.0) >= min_balance]


def transfer_funds(accounts: List[Dict[str, Union[str, float]]], from_id: str, to_id: str, amount: float) -> None:
    """Transfer funds between two accounts in-place."""
    if amount <= 0:
        raise ValueError("Transfer amount must be positive")

    from_acct = next((a for a in accounts if a.get('account_id') == from_id), None)
    to_acct = next((a for a in accounts if a.get('account_id') == to_id), None)

    if not from_acct or not to_acct:
        raise ValueError("Both source and target accounts must exist")

    if from_acct.get('balance', 0.0) < amount:
        raise ValueError("Insufficient funds")

    from_acct['balance'] -= amount
    to_acct['balance'] += amount


def high_value_account_ids(accounts: List[Dict[str, Union[str, float]]], min_balance: float = 10000.0) -> List[str]:
    """Get account_ids for high balance accounts."""
    return [acct['account_id'] for acct in accounts if acct.get('balance', 0.0) >= min_balance]


def find_account(accounts: List[Dict[str, Union[str, float]]], account_id: str) -> Optional[Dict[str, Union[str, float]]]:
    """Find a single account by id."""
    return next((acct for acct in accounts if acct.get('account_id') == account_id), None)


def example() -> None:
    """Demonstrate basic banking operations."""
    accounts = [
        {"account_id": "A001", "balance": 10250.75, "status": "active", "type": "savings"},
        {"account_id": "A002", "balance": 500.0, "status": "inactive", "type": "checking"},
    ]

    print("--- BASIC MODULE EXAMPLE ---")
    print("Simple interest:", calculate_interest(10000.0, 3.5, 2))
    print("Compound interest:", calculate_compound_interest(10000.0, 0.05, 12, 5))
    print(summarize_account(accounts[0]))
    print("High value IDs:", high_value_account_ids(accounts, min_balance=1000.0))
    transfer_funds(accounts, "A001", "A002", 250.0)
    print("After transfer:", accounts)
