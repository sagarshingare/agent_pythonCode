"""Math utilities with banking examples, including financial formulas and common Python math operations."""

import math
from typing import List


def calculate_compound_interest(principal: float, rate: float, times_per_year: int, years: float) -> float:
    """Compute compound interest formula using floating point math."""
    if principal < 0 or rate < 0 or times_per_year <= 0 or years < 0:
        raise ValueError("principal, rate, times_per_year and years must be non-negative and times_per_year > 0")
    return principal * ((1 + rate / times_per_year) ** (times_per_year * years))


def present_value(future_value: float, discount_rate: float, periods: int) -> float:
    """Compute present value of future cashflow."""
    if periods < 0:
        raise ValueError("periods must be non-negative")
    return future_value / ((1 + discount_rate) ** periods)


def future_value(present_value_amount: float, rate: float, periods: int) -> float:
    """Compute future value of a present sum."""
    if periods < 0:
        raise ValueError("periods must be non-negative")
    return present_value_amount * ((1 + rate) ** periods)


def loan_payment(principal: float, annual_rate: float, term_months: int) -> float:
    """Calculate amortized monthly loan payment."""
    if term_months <= 0:
        raise ValueError("term_months must be positive")
    monthly_rate = annual_rate / 12.0
    if monthly_rate == 0:
        return principal / term_months
    factor = (monthly_rate * (1 + monthly_rate) ** term_months) / ((1 + monthly_rate) ** term_months - 1)
    return principal * factor


def percentile(data: List[float], percentile_value: float) -> float:
    """Compute percentile using linear interpolation."""
    if not 0 <= percentile_value <= 100:
        raise ValueError("percentile_value must be between 0 and 100")
    n = len(data)
    if n == 0:
        raise ValueError("data list cannot be empty")
    sorted_data = sorted(data)
    rank = percentile_value / 100 * (n - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    weight = rank - lower
    if lower == upper:
        return sorted_data[int(rank)]
    return sorted_data[lower] * (1 - weight) + sorted_data[upper] * weight


def quantize_currency(amount: float, decimals: int = 2) -> float:
    """Round amounts to currency precision."""
    return round(amount, decimals)


def example() -> None:
    """Math module example for banking formulas."""
    print("--- MATH LIBS MODULE EXAMPLE ---")
    print("Compound interest:", calculate_compound_interest(10000, 0.05, 12, 5))
    print("Present value:", present_value(100000, 0.05, 5))
    print("Future value:", future_value(10000, 0.05, 5))
    print("Loan payment:", loan_payment(250000, 0.04, 360))
    print("90th percentile:", percentile([1, 2, 2, 3, 5, 8], 90))
    print("Rounded Currency:", quantize_currency(1234.5678))
