"""
Main Orchestration Module for Wealth Management POC

Integrates all modules to provide a comprehensive wealth management solution
with data ingestion, analysis, risk assessment, and predictive modeling.

Python Version: 3.9.6
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import warnings
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_ingestion import load_stock_data, create_sample_portfolio
from portfolio_analysis import PortfolioAnalyzer
from risk_assessment import RiskAnalyzer
from predictive_modeling import FinancialPredictor, PortfolioOptimizer
from pyspark_processing import get_spark_session, load_portfolio_data, calculate_portfolio_metrics

# Optional imports
try:
    from pyspark_processing import aggregate_by_sector
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    warnings.warn("PySpark processing will be skipped.")


class WealthManagementSystem:
    """
    Main class orchestrating the wealth management POC system.
    """

    def __init__(self):
        """Initialize the wealth management system."""
        self.portfolio_data = None
        self.market_data = None
        self.portfolio_analyzer = None
        self.risk_analyzer = None
        self.spark_session = None

    def initialize_system(self) -> None:
        """Initialize all system components."""
        global PYSPARK_AVAILABLE
        print("Initializing Wealth Management System...")

        # Load sample data
        self._load_sample_data()

        # Initialize analyzers
        self.portfolio_analyzer = PortfolioAnalyzer(self.portfolio_data)
        self.risk_analyzer = RiskAnalyzer()

        # Initialize Spark if available
        if PYSPARK_AVAILABLE:
            self.spark_session = get_spark_session("WealthManagementPOC")
            if self.spark_session is not None:
                print("Spark session initialized.")
            else:
                print("Spark session unavailable or incompatible Java version detected. Spark features will be skipped.")
                PYSPARK_AVAILABLE = False
        else:
            print("Spark not available - some features will be limited.")

        print("System initialization complete.\n")

    def _load_sample_data(self) -> None:
        """Load sample portfolio and market data."""
        print("Loading sample data...")

        # Load portfolio data
        self.portfolio_data = create_sample_portfolio()
        print(f"Loaded portfolio with {len(self.portfolio_data)} holdings")

        # Load market data
        symbols = self.portfolio_data['symbol'].tolist()
        self.market_data = load_stock_data(symbols, period="1y")
        print(f"Loaded market data for {len(symbols)} symbols")

    def run_portfolio_analysis(self) -> Dict:
        """Run comprehensive portfolio analysis."""
        print("=== Portfolio Analysis ===")

        if self.portfolio_analyzer is None:
            raise ValueError("System not initialized. Call initialize_system() first.")

        # Get portfolio summary
        summary = self.portfolio_analyzer.get_portfolio_summary()
        print("Portfolio Summary:")
        print(f"  Total Value: ${summary['total_value']:,.2f}")
        print(f"  Total Cost Basis: ${summary['total_cost_basis']:,.2f}")
        print(f"  Total Gain/Loss: ${summary['total_gain_loss']:,.2f}")
        print(f"  Total Return: {summary['total_return_pct']:.2%}")
        print(f"  Number of Holdings: {summary['num_holdings']}")

        # Sector allocation
        sector_alloc = self.portfolio_analyzer.get_sector_allocation()
        if not sector_alloc.empty:
            print("\nSector Allocation:")
            for sector, row in sector_alloc.iterrows():
                print(f"  {sector}: ${row['total_value']:,.0f} ({row['allocation_pct']:.1f}%)")

        # Performance distribution
        perf_dist = self.portfolio_analyzer.get_performance_distribution()
        print("\nPerformance Distribution:")
        print(f"  Mean Return: {perf_dist['mean_return']:.2f}%")
        print(f"  Best Performer: {summary['best_performer']}")
        print(f"  Worst Performer: {summary['worst_performer']}")

        # Recommendations
        recommendations = self.portfolio_analyzer.get_recommendations()
        print("\nRecommendations:")
        for rec in recommendations:
            print(f"  • {rec}")

        return {
            'summary': summary,
            'sector_allocation': sector_alloc,
            'performance_distribution': perf_dist,
            'recommendations': recommendations
        }

    def run_risk_assessment(self) -> Dict:
        """Run comprehensive risk assessment."""
        print("\n=== Risk Assessment ===")

        if self.market_data is None:
            print("No market data available for risk assessment.")
            return {}

        # Calculate returns for risk analysis
        if 'close' in self.market_data.columns:
            # Assuming market_data has a 'close' column for one symbol
            returns = self.market_data['close'].pct_change().dropna()
        else:
            # Generate sample returns
            np.random.seed(42)
            returns = pd.Series(np.random.normal(0.001, 0.02, 252))

        portfolio_value = self.portfolio_analyzer.get_portfolio_summary()['total_value']

        # Calculate different VaR measures
        hist_var = self.risk_analyzer.calculate_historical_var(returns, portfolio_value)
        param_var = self.risk_analyzer.calculate_parametric_var(returns, portfolio_value)
        mc_var = self.risk_analyzer.calculate_monte_carlo_var(returns, portfolio_value)

        print("Value at Risk (95% confidence):")
        print(f"  Historical VaR: ${hist_var['var_amount']:,.0f}")
        print(f"  Parametric VaR: ${param_var['var_amount']:,.0f}")
        print(f"  Monte Carlo VaR: ${mc_var['var_amount']:,.0f}")

        # Risk ratios
        sharpe = self.risk_analyzer.calculate_sharpe_ratio(returns)
        sortino = self.risk_analyzer.calculate_sortino_ratio(returns)

        print("Risk-Adjusted Returns:")
        print(f"  Sharpe Ratio: {sharpe:.2f}")
        print(f"  Sortino Ratio: {sortino:.2f}")

        # Maximum drawdown
        prices = np.cumprod(1 + returns)
        max_dd = self.risk_analyzer.calculate_max_drawdown(prices)
        print(f"  Maximum Drawdown: {max_dd['max_drawdown']:.2%}")

        return {
            'historical_var': hist_var,
            'parametric_var': param_var,
            'monte_carlo_var': mc_var,
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
            'max_drawdown': max_dd
        }

    def run_predictive_modeling(self) -> Dict:
        """Run predictive modeling for price forecasting."""
        print("\n=== Predictive Modeling ===")

        if self.market_data is None or len(self.market_data) < 50:
            print("Insufficient market data for predictive modeling.")
            return {}

        # Prepare data for prediction
        predictor = FinancialPredictor(model_type='rf')

        # Use available columns as features
        available_cols = [col for col in ['open', 'high', 'low', 'close', 'volume']
                         if col in self.market_data.columns]

        if len(available_cols) < 2:
            print("Insufficient features for modeling.")
            return {}

        X, y = predictor.prepare_features(self.market_data, 'close', available_cols, lag_features=3)

        if len(X) < 30:
            print("Insufficient data points for modeling.")
            return {}

        # Split and train
        X_train, X_test, y_train, y_test = predictor.train_test_split_temporal(X, y)

        train_metrics = predictor.fit(X_train, y_train)
        test_metrics = predictor.evaluate(X_test, y_test)

        print("Model Performance:")
        print(f"  Training R²: {train_metrics.get('r2', 'N/A')}")
        print(f"  Test R²: {test_metrics['r2']}")
        print(f"  Test RMSE: {test_metrics['rmse']:.2f}")
        print(f"  Directional Accuracy: {test_metrics['directional_accuracy']:.1%}")

        # Portfolio optimization
        print("\nPortfolio Optimization:")
        # Generate sample returns for optimization demo
        np.random.seed(42)
        asset_returns = pd.DataFrame({
            name: np.random.normal(0.001, 0.02, 252)
            for name in self.portfolio_data['symbol'].head(4)
        })

        optimizer = PortfolioOptimizer(asset_returns)
        optimal_portfolios = optimizer.optimize_portfolio()

        max_sharpe = optimal_portfolios['max_sharpe_portfolio']
        print("Optimal Portfolio (Max Sharpe Ratio):")
        for i, symbol in enumerate(asset_returns.columns):
            weight = max_sharpe['weights'][i]
            print(".1%")
        print(".2%")
        print(".2f")

        return {
            'model_performance': test_metrics,
            'optimal_portfolio': max_sharpe,
            'feature_importance': predictor.get_feature_importance()
        }

    def run_pyspark_processing(self) -> Optional[Dict]:
        """Run PySpark-based data processing if available."""
        if not PYSPARK_AVAILABLE or self.spark_session is None:
            print("\nPySpark processing skipped (not available).")
            return None

        print("\n=== PySpark Data Processing ===")

        try:
            # Load portfolio data into Spark
            spark_df = load_portfolio_data(self.spark_session)

            # Calculate metrics
            metrics_df = calculate_portfolio_metrics(spark_df)

            # Aggregate by sector
            sector_agg = aggregate_by_sector(metrics_df)

            print("Spark Processing Results:")
            print("Portfolio Metrics:")
            metrics_df.select("symbol", "position_value", "gain_loss_pct").show()

            print("Sector Aggregation:")
            sector_agg.show()

            total_value = metrics_df.agg({"position_value": "sum"}).collect()[0][0]
            print(".2f")

            return {
                'portfolio_metrics': metrics_df,
                'sector_aggregation': sector_agg,
                'total_value': total_value
            }

        except Exception as e:
            print(f"Error in PySpark processing: {e}")
            return None
        finally:
            self.spark_session.stop()

    def generate_comprehensive_report(self) -> str:
        """Generate a comprehensive wealth management report."""
        print("\n=== Comprehensive Wealth Management Report ===")

        report = []
        report.append("# Wealth Management Analysis Report")
        report.append(f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # Portfolio Analysis
        portfolio_results = self.run_portfolio_analysis()
        if portfolio_results:
            summary = portfolio_results['summary']
            report.append("## Portfolio Overview")
            report.append(f"- Total Portfolio Value: ${summary['total_value']:,.2f}")
            report.append(f"- Total Return: {summary['total_return_pct']:.2f}%")
            report.append(f"- Number of Holdings: {summary['num_holdings']}")
            report.append("")

        # Risk Assessment
        risk_results = self.run_risk_assessment()
        if risk_results:
            report.append("## Risk Analysis")
            hist_var = risk_results['historical_var']
            report.append(f"- Value at Risk (95%): ${hist_var['var_amount']:,.0f}")
            report.append(f"- Sharpe Ratio: {risk_results['sharpe_ratio']:.2f}")
            report.append(f"- Maximum Drawdown: {risk_results['max_drawdown']['max_drawdown']:.2%}")
            report.append("")

        # Predictive Modeling
        pred_results = self.run_predictive_modeling()
        if pred_results:
            report.append("## Predictive Analytics")
            perf = pred_results['model_performance']
            report.append(f"- Model Accuracy (R²): {perf['r2']:.3f}")
            report.append(f"- Directional Accuracy: {perf['directional_accuracy']:.1%}")
            report.append("")

        # PySpark Processing
        spark_results = self.run_pyspark_processing()
        if spark_results:
            report.append("## Big Data Processing")
            report.append(f"- Total Portfolio Value (Spark): ${spark_results['total_value']:,.2f}")
            report.append("")

        report.append("## Recommendations")
        if portfolio_results and 'recommendations' in portfolio_results:
            for rec in portfolio_results['recommendations']:
                report.append(f"- {rec}")
        else:
            report.append("- Portfolio appears well-balanced.")

        return "\n".join(report)

    def run_full_analysis(self) -> None:
        """Run complete analysis pipeline."""
        try:
            self.initialize_system()
            self.run_portfolio_analysis()
            self.run_risk_assessment()
            self.run_predictive_modeling()
            self.run_pyspark_processing()

            # Generate report
            report = self.generate_comprehensive_report()
            print("\n" + "="*60)
            print(report)
            print("="*60)

        except Exception as e:
            print(f"Error during analysis: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Main entry point for the wealth management POC."""
    print("Wealth Management Proof-of-Concept System")
    print("=" * 50)

    # Initialize and run system
    wms = WealthManagementSystem()
    wms.run_full_analysis()

    print("\nAnalysis complete. Check the output above for detailed results.")


if __name__ == "__main__":
    main()