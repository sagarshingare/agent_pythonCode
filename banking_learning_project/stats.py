"""Statistics utilities for banking use, with numpy/scipy statistical methods."""

from typing import List
import numpy as np
import scipy.stats as stats


def portfolio_return_statistics(returns: List[float]) -> dict:
    """Compute mean, variance, stddev, skewness, kurtosis.

    Uses numpy + scipy; Spark version equivalent is DataFrame.summary.
    """
    if not returns:
        raise ValueError("returns list is empty")
    arr = np.array(returns, dtype=float)
    mean = float(np.mean(arr))
    var = float(np.var(arr, ddof=1))
    std = float(np.std(arr, ddof=1))
    skew = float(stats.skew(arr))
    kurtosis = float(stats.kurtosis(arr))
    return {
        "mean": mean,
        "variance": var,
        "std_dev": std,
        "skewness": skew,
        "kurtosis": kurtosis,
    }


def value_at_risk(returns: List[float], confidence_level: float = 0.95) -> float:
    """Historical VaR, representing potential loss at confidence level."""
    if not 0 < confidence_level < 1:
        raise ValueError("confidence_level must be between 0 and 1")
    arr = np.array(returns, dtype=float)
    if arr.size == 0:
        raise ValueError("returns list is empty")
    return float(-np.percentile(arr, 100 * (1 - confidence_level)))


def expected_shortfall(returns: List[float], confidence_level: float = 0.95) -> float:
    """Expected shortfall (CVaR) at a confidence level."""
    var_val = value_at_risk(returns, confidence_level)
    arr = np.array(returns, dtype=float)
    losses = arr[arr <= -var_val]
    if losses.size == 0:
        return 0.0
    return float(-np.mean(losses))


def sharpe_ratio(returns: List[float], risk_free_rate: float = 0.0, periods: int = 252) -> float:
    """Calculate annualized Sharpe ratio."""
    if not returns:
        raise ValueError("returns list is empty")
    arr = np.array(returns, dtype=float)
    excess = arr - risk_free_rate / periods
    return float(np.sqrt(periods) * np.mean(excess) / np.std(excess, ddof=1))


def regression_metrics(actual: List[float], predicted: List[float]) -> dict:
    """Compute common regression metrics (MSE, RMSE, MAE, R2)."""
    if len(actual) != len(predicted):
        raise ValueError("actual and predicted arrays must have same length")
    y = np.array(actual, dtype=float)
    y_pred = np.array(predicted, dtype=float)
    residuals = y - y_pred
    mse = float(np.mean(residuals ** 2))
    rmse = float(np.sqrt(mse))
    mae = float(np.mean(np.abs(residuals)))
    ss_res = np.sum(residuals ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = float(1 - ss_res / ss_tot if ss_tot != 0 else 0.0)
    return {
        "mse": mse,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
    }


def example() -> None:
    """Statistics module example for banking return series."""
    returns = [0.01, -0.02, 0.015, 0.03, -0.01, 0.005]
    print("--- STATS MODULE EXAMPLE ---")
    print(portfolio_return_statistics(returns))
    print("VaR 95%:", value_at_risk(returns, 0.95))
    print("ES 95%:", expected_shortfall(returns, 0.95))
    print("Sharpe ratio:", sharpe_ratio(returns, risk_free_rate=0.01))
    print("Regression:", regression_metrics([1, 2, 3], [1.1, 1.9, 3.05]))
