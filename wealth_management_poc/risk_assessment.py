"""
Risk Assessment Module for Wealth Management

Provides comprehensive risk analysis including Value at Risk (VaR),
Conditional VaR, Sharpe ratio, and other risk metrics.

Python Version: 3.9.6
Dependencies: numpy 1.21+, scipy 1.7+, pandas 1.5+, statsmodels 0.13+
"""

import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import norm, t
from typing import Dict, List, Tuple, Optional, Union
import warnings


class RiskAnalyzer:
    """
    Class for calculating various risk metrics for portfolios and individual assets.
    """

    def __init__(self, returns_data: Optional[pd.DataFrame] = None,
                 confidence_level: float = 0.95):
        """
        Initialize risk analyzer.

        Args:
            returns_data: DataFrame with historical returns (optional)
            confidence_level: Confidence level for VaR calculations (default 95%)
        """
        self.returns_data = returns_data
        self.confidence_level = confidence_level
        self.alpha = 1 - confidence_level

    def calculate_historical_var(self, returns: Union[pd.Series, np.ndarray],
                               portfolio_value: float = 1000000) -> Dict:
        """
        Calculate Historical Value at Risk (VaR).

        Args:
            returns: Historical returns series
            portfolio_value: Current portfolio value

        Returns:
            Dictionary with VaR metrics
        """
        returns = np.array(returns)
        returns = returns[~np.isnan(returns)]  # Remove NaN values

        if len(returns) < 30:
            warnings.warn("Limited historical data. VaR estimates may be unreliable.")

        # Sort returns in ascending order (worst to best)
        sorted_returns = np.sort(returns)

        # Find the return at the alpha percentile
        var_percentile = np.percentile(sorted_returns, self.alpha * 100)
        var_amount = portfolio_value * abs(var_percentile)

        # Expected Shortfall (Conditional VaR)
        tail_losses = sorted_returns[sorted_returns <= var_percentile]
        cvar = np.mean(tail_losses) if len(tail_losses) > 0 else var_percentile
        cvar_amount = portfolio_value * abs(cvar)

        return {
            'var_return': round(var_percentile, 4),
            'var_amount': round(var_amount, 2),
            'cvar_return': round(cvar, 4),
            'cvar_amount': round(cvar_amount, 2),
            'confidence_level': self.confidence_level,
            'historical_periods': len(returns)
        }

    def calculate_parametric_var(self, returns: Union[pd.Series, np.ndarray],
                               portfolio_value: float = 1000000,
                               use_t_distribution: bool = False) -> Dict:
        """
        Calculate Parametric (Variance-Covariance) Value at Risk.

        Args:
            returns: Historical returns series
            portfolio_value: Current portfolio value
            use_t_distribution: Whether to use t-distribution instead of normal

        Returns:
            Dictionary with parametric VaR metrics
        """
        returns = np.array(returns)
        returns = returns[~np.isnan(returns)]

        mu = np.mean(returns)
        sigma = np.std(returns, ddof=1)

        if use_t_distribution and len(returns) > 2:
            # Fit t-distribution
            df, loc, scale = stats.t.fit(returns)
            var_return = stats.t.ppf(self.alpha, df, loc, scale)
        else:
            # Normal distribution
            var_return = norm.ppf(self.alpha, mu, sigma)

        var_amount = portfolio_value * abs(var_return)

        return {
            'var_return': round(var_return, 4),
            'var_amount': round(var_amount, 2),
            'mean_return': round(mu, 4),
            'volatility': round(sigma, 4),
            'distribution': 't' if use_t_distribution else 'normal',
            'confidence_level': self.confidence_level
        }

    def calculate_monte_carlo_var(self, returns: Union[pd.Series, np.ndarray],
                                portfolio_value: float = 1000000,
                                num_simulations: int = 10000) -> Dict:
        """
        Calculate Monte Carlo Value at Risk.

        Args:
            returns: Historical returns series
            portfolio_value: Current portfolio value
            num_simulations: Number of Monte Carlo simulations

        Returns:
            Dictionary with Monte Carlo VaR metrics
        """
        returns = np.array(returns)
        returns = returns[~np.isnan(returns)]

        mu = np.mean(returns)
        sigma = np.std(returns, ddof=1)

        # Generate random returns
        simulated_returns = np.random.normal(mu, sigma, num_simulations)

        # Calculate portfolio values
        simulated_values = portfolio_value * (1 + simulated_returns)

        # Find VaR
        var_value = np.percentile(simulated_values, self.alpha * 100)
        var_amount = portfolio_value - var_value

        # Expected Shortfall
        tail_values = simulated_values[simulated_values <= var_value]
        cvar_value = np.mean(tail_values) if len(tail_values) > 0 else var_value
        cvar_amount = portfolio_value - cvar_value

        return {
            'var_amount': round(var_amount, 2),
            'cvar_amount': round(cvar_amount, 2),
            'worst_case': round(portfolio_value - simulated_values.min(), 2),
            'best_case': round(simulated_values.max() - portfolio_value, 2),
            'simulations': num_simulations,
            'confidence_level': self.confidence_level
        }

    def calculate_sharpe_ratio(self, returns: Union[pd.Series, np.ndarray],
                             risk_free_rate: float = 0.02) -> float:
        """
        Calculate Sharpe ratio.

        Args:
            returns: Historical returns series
            risk_free_rate: Risk-free rate (annualized)

        Returns:
            Sharpe ratio
        """
        returns = np.array(returns)
        returns = returns[~np.isnan(returns)]

        if len(returns) == 0:
            return 0.0

        excess_returns = returns - risk_free_rate/252  # Assuming daily returns
        avg_excess_return = np.mean(excess_returns)
        volatility = np.std(excess_returns, ddof=1)

        return round(avg_excess_return / volatility * np.sqrt(252), 2) if volatility > 0 else 0.0

    def calculate_sortino_ratio(self, returns: Union[pd.Series, np.ndarray],
                              risk_free_rate: float = 0.02) -> float:
        """
        Calculate Sortino ratio (downside deviation).

        Args:
            returns: Historical returns series
            risk_free_rate: Risk-free rate (annualized)

        Returns:
            Sortino ratio
        """
        returns = np.array(returns)
        returns = returns[~np.isnan(returns)]

        excess_returns = returns - risk_free_rate/252

        # Downside deviation (only negative excess returns)
        downside_returns = excess_returns[excess_returns < 0]
        downside_deviation = np.std(downside_returns, ddof=1) if len(downside_returns) > 0 else 0

        avg_excess_return = np.mean(excess_returns)

        return round(avg_excess_return / downside_deviation * np.sqrt(252), 2) if downside_deviation > 0 else 0.0

    def calculate_max_drawdown(self, prices: Union[pd.Series, np.ndarray]) -> Dict:
        """
        Calculate maximum drawdown.

        Args:
            prices: Historical price series

        Returns:
            Dictionary with drawdown metrics
        """
        prices = np.array(prices)
        prices = prices[~np.isnan(prices)]

        if len(prices) < 2:
            return {'max_drawdown': 0, 'peak': prices[0] if len(prices) > 0 else 0, 'trough': prices[0] if len(prices) > 0 else 0}

        # Calculate cumulative returns
        cumulative = np.cumprod(1 + np.diff(prices) / prices[:-1])

        # Calculate running maximum
        running_max = np.maximum.accumulate(cumulative)

        # Calculate drawdown
        drawdown = (cumulative - running_max) / running_max

        max_drawdown_idx = np.argmin(drawdown)
        peak_idx = np.where(running_max == running_max[max_drawdown_idx])[0][0]

        return {
            'max_drawdown': round(abs(drawdown[max_drawdown_idx]), 4),
            'peak_value': round(running_max[peak_idx], 2),
            'trough_value': round(cumulative[max_drawdown_idx], 2),
            'recovery_period': max_drawdown_idx - peak_idx
        }

    def get_portfolio_risk_metrics(self, portfolio_returns: pd.DataFrame,
                                 portfolio_weights: np.ndarray) -> Dict:
        """
        Calculate portfolio-level risk metrics.

        Args:
            portfolio_returns: DataFrame with asset returns
            portfolio_weights: Array of portfolio weights

        Returns:
            Dictionary with portfolio risk metrics
        """
        # Calculate portfolio returns
        portfolio_return = portfolio_returns.dot(portfolio_weights)

        # Calculate covariance matrix
        cov_matrix = portfolio_returns.cov()

        # Portfolio volatility
        portfolio_volatility = np.sqrt(portfolio_weights.T @ cov_matrix @ portfolio_weights)

        # Portfolio VaR
        portfolio_var = self.calculate_historical_var(portfolio_return)

        return {
            'portfolio_volatility': round(portfolio_volatility * np.sqrt(252), 4),  # Annualized
            'portfolio_sharpe': self.calculate_sharpe_ratio(portfolio_return),
            'portfolio_var': portfolio_var,
            'diversification_ratio': np.sqrt(portfolio_weights.T @ cov_matrix @ portfolio_weights) / np.sum(np.abs(portfolio_weights) * np.sqrt(np.diag(cov_matrix)))
        }


def generate_sample_returns(num_days: int = 252, num_assets: int = 5) -> pd.DataFrame:
    """
    Generate sample return data for testing.

    Args:
        num_days: Number of trading days
        num_assets: Number of assets

    Returns:
        DataFrame with sample returns
    """
    np.random.seed(42)

    # Generate correlated returns
    base_returns = np.random.normal(0.0005, 0.02, (num_days, num_assets))

    # Add some correlation
    correlation_matrix = np.array([
        [1.0, 0.6, 0.3, 0.2, 0.1],
        [0.6, 1.0, 0.4, 0.3, 0.2],
        [0.3, 0.4, 1.0, 0.5, 0.3],
        [0.2, 0.3, 0.5, 1.0, 0.4],
        [0.1, 0.2, 0.3, 0.4, 1.0]
    ])

    # Apply correlation
    chol = np.linalg.cholesky(correlation_matrix)
    correlated_returns = base_returns @ chol.T

    asset_names = [f'Asset_{i+1}' for i in range(num_assets)]
    return pd.DataFrame(correlated_returns, columns=asset_names)


def example() -> None:
    """Example usage of risk assessment module."""
    print("=== Risk Assessment Example ===")

    # Generate sample returns
    returns_df = generate_sample_returns()
    asset_returns = returns_df['Asset_1']

    # Initialize risk analyzer
    analyzer = RiskAnalyzer(confidence_level=0.95)

    # Calculate VaR using different methods
    print("Value at Risk Calculations (95% confidence, $1M portfolio):")

    hist_var = analyzer.calculate_historical_var(asset_returns)
    print(f"Historical VaR: ${hist_var['var_amount']:,.0f} ({hist_var['var_return']:.2%})")

    param_var = analyzer.calculate_parametric_var(asset_returns)
    print(f"Parametric VaR: ${param_var['var_amount']:,.0f} ({param_var['var_return']:.2%})")

    mc_var = analyzer.calculate_monte_carlo_var(asset_returns)
    print(f"Monte Carlo VaR: ${mc_var['var_amount']:,.0f}")

    # Risk ratios
    sharpe = analyzer.calculate_sharpe_ratio(asset_returns)
    sortino = analyzer.calculate_sortino_ratio(asset_returns)

    print(f"\nSharpe Ratio: {sharpe}")
    print(f"Sortino Ratio: {sortino}")

    # Maximum drawdown (using cumulative prices)
    prices = np.cumprod(1 + asset_returns)
    max_dd = analyzer.calculate_max_drawdown(prices)
    print(f"\nMaximum Drawdown: {max_dd['max_drawdown']:.2%}")

    # Portfolio risk metrics
    portfolio_weights = np.array([0.3, 0.25, 0.2, 0.15, 0.1])
    port_risk = analyzer.get_portfolio_risk_metrics(returns_df, portfolio_weights)

    print("\nPortfolio Risk Metrics:")
    print(f"Portfolio Volatility (annualized): {port_risk['portfolio_volatility']:.2%}")
    print(f"Portfolio Sharpe Ratio: {port_risk['portfolio_sharpe']}")
    print(f"Portfolio VaR: ${port_risk['portfolio_var']['var_amount']:,.0f}")


if __name__ == "__main__":
    example()