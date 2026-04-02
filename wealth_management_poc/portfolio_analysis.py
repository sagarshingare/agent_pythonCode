"""
Portfolio Analysis Module for Wealth Management

Provides statistical analysis and performance metrics for investment portfolios.

Python Version: 3.9.6
Dependencies: pandas 1.5+, numpy 1.21+, matplotlib 3.5+ (optional for plotting)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from scipy import stats
import warnings

try:
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    warnings.warn("matplotlib not available. Plotting functions will be disabled.")


class PortfolioAnalyzer:
    """
    Class for analyzing investment portfolio performance and risk metrics.
    """

    def __init__(self, portfolio_df: pd.DataFrame):
        """
        Initialize with portfolio data.

        Args:
            portfolio_df: DataFrame with columns: symbol, shares, purchase_price, current_price
        """
        self.portfolio = portfolio_df.copy()
        self._calculate_basic_metrics()

    def _calculate_basic_metrics(self) -> None:
        """Calculate basic portfolio metrics."""
        self.portfolio['position_value'] = self.portfolio['shares'] * self.portfolio['current_price']
        self.portfolio['cost_basis'] = self.portfolio['shares'] * self.portfolio['purchase_price']
        self.portfolio['gain_loss'] = self.portfolio['position_value'] - self.portfolio['cost_basis']
        self.portfolio['gain_loss_pct'] = (self.portfolio['gain_loss'] / self.portfolio['cost_basis']) * 100

    def get_portfolio_summary(self) -> Dict:
        """
        Get overall portfolio summary statistics.

        Returns:
            Dictionary with portfolio summary metrics
        """
        total_value = self.portfolio['position_value'].sum()
        total_cost = self.portfolio['cost_basis'].sum()
        total_gain_loss = self.portfolio['gain_loss'].sum()
        total_return_pct = (total_gain_loss / total_cost) * 100

        return {
            'total_value': round(total_value, 2),
            'total_cost_basis': round(total_cost, 2),
            'total_gain_loss': round(total_gain_loss, 2),
            'total_return_pct': round(total_return_pct, 2),
            'num_holdings': len(self.portfolio),
            'best_performer': self.portfolio.loc[self.portfolio['gain_loss_pct'].idxmax(), 'symbol'],
            'worst_performer': self.portfolio.loc[self.portfolio['gain_loss_pct'].idxmin(), 'symbol']
        }

    def get_sector_allocation(self) -> pd.DataFrame:
        """
        Get portfolio allocation by sector.

        Returns:
            DataFrame with sector allocation
        """
        if 'sector' not in self.portfolio.columns:
            return pd.DataFrame()

        sector_allocation = self.portfolio.groupby('sector').agg({
            'position_value': 'sum',
            'cost_basis': 'sum',
            'gain_loss': 'sum'
        }).round(2)

        sector_allocation = sector_allocation.rename(columns={
            'position_value': 'total_value',
            'cost_basis': 'total_cost_basis',
            'gain_loss': 'total_gain_loss'
        })

        sector_allocation['allocation_pct'] = (sector_allocation['total_value'] /
                                               sector_allocation['total_value'].sum() * 100).round(2)

        return sector_allocation.sort_values('allocation_pct', ascending=False)

    def calculate_weighted_metrics(self) -> Dict:
        """
        Calculate weighted average metrics for the portfolio.

        Returns:
            Dictionary with weighted metrics
        """
        total_value = self.portfolio['position_value'].sum()

        # Weight by position value
        weights = self.portfolio['position_value'] / total_value

        weighted_return = (weights * self.portfolio['gain_loss_pct']).sum()

        # Calculate weighted volatility (simplified)
        # In practice, this would require historical price data
        avg_volatility = self.portfolio['gain_loss_pct'].std()  # Simplified

        return {
            'weighted_avg_return': round(weighted_return, 2),
            'portfolio_volatility': round(avg_volatility, 2),
            'sharpe_ratio': round(weighted_return / avg_volatility, 2) if avg_volatility > 0 else 0
        }

    def get_performance_distribution(self) -> Dict:
        """
        Analyze the distribution of returns across holdings.

        Returns:
            Dictionary with distribution statistics
        """
        returns = self.portfolio['gain_loss_pct']

        return {
            'mean_return': round(returns.mean(), 2),
            'median_return': round(returns.median(), 2),
            'std_return': round(returns.std(), 2),
            'min_return': round(returns.min(), 2),
            'max_return': round(returns.max(), 2),
            'positive_holdings': (returns > 0).sum(),
            'negative_holdings': (returns < 0).sum(),
            'return_skewness': round(stats.skew(returns), 2),
            'return_kurtosis': round(stats.kurtosis(returns), 2)
        }

    def plot_portfolio_composition(self, save_path: Optional[str] = None) -> None:
        """
        Plot portfolio composition by sector (if matplotlib available).

        Args:
            save_path: Path to save the plot (optional)
        """
        if not MATPLOTLIB_AVAILABLE:
            print("Matplotlib not available for plotting.")
            return

        sector_data = self.get_sector_allocation()
        if sector_data.empty:
            print("No sector data available for plotting.")
            return

        plt.figure(figsize=(10, 6))
        plt.pie(sector_data['allocation_pct'], labels=sector_data.index,
                autopct='%1.1f%%', startangle=90)
        plt.title('Portfolio Allocation by Sector')
        plt.axis('equal')

        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()

    def get_recommendations(self) -> List[str]:
        """
        Generate basic portfolio recommendations based on analysis.

        Returns:
            List of recommendation strings
        """
        recommendations = []
        summary = self.get_portfolio_summary()
        sector_alloc = self.get_sector_allocation()

        # Check diversification
        if len(sector_alloc) < 3:
            recommendations.append("Consider diversifying across more sectors to reduce risk.")

        # Check concentration
        max_allocation = sector_alloc['allocation_pct'].max() if not sector_alloc.empty else 0
        if max_allocation > 50:
            recommendations.append("High concentration in a single sector detected; consider rebalancing to reduce risk.")

        # Check performance
        if summary['total_return_pct'] < 0:
            recommendations.append("Portfolio is showing negative returns. Consider rebalancing or tax-loss harvesting.")

        # Check number of holdings
        if summary['num_holdings'] < 5:
            recommendations.append("Consider adding more holdings for better diversification.")

        return recommendations if recommendations else ["Portfolio appears well-balanced."]


def calculate_correlation_matrix(price_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate correlation matrix for asset returns.

    Args:
        price_data: DataFrame with price data (columns = assets, index = dates)

    Returns:
        Correlation matrix DataFrame
    """
    # Calculate returns
    returns = price_data.pct_change().dropna()
    return returns.corr().round(3)


def example() -> None:
    """Example usage of portfolio analysis module."""
    print("=== Portfolio Analysis Example ===")

    # Create sample portfolio
    portfolio_data = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'BAC'],
        'shares': [100, 50, 25, 30, 20, 40, 75, 100],
        'purchase_price': [150, 250, 2800, 3200, 800, 400, 120, 35],
        'current_price': [175, 280, 3100, 3400, 950, 480, 135, 32],
        'sector': ['Tech', 'Tech', 'Tech', 'Consumer', 'Auto', 'Tech', 'Finance', 'Finance']
    })

    # Initialize analyzer
    analyzer = PortfolioAnalyzer(portfolio_data)

    # Get summary
    summary = analyzer.get_portfolio_summary()
    print("Portfolio Summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")

    # Get sector allocation
    sector_alloc = analyzer.get_sector_allocation()
    print("\nSector Allocation:")
    print(sector_alloc)

    # Get weighted metrics
    weighted_metrics = analyzer.calculate_weighted_metrics()
    print("\nWeighted Metrics:")
    for key, value in weighted_metrics.items():
        print(f"  {key}: {value}")

    # Get performance distribution
    perf_dist = analyzer.get_performance_distribution()
    print("\nPerformance Distribution:")
    for key, value in perf_dist.items():
        print(f"  {key}: {value}")

    # Get recommendations
    recommendations = analyzer.get_recommendations()
    print("\nRecommendations:")
    for rec in recommendations:
        print(f"  - {rec}")

    # Plot if possible
    if MATPLOTLIB_AVAILABLE:
        print("\nGenerating portfolio composition plot...")
        analyzer.plot_portfolio_composition()


if __name__ == "__main__":
    example()