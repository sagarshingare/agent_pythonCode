"""
Tests for Wealth Management POC
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

# Import modules to test
from data_ingestion import generate_sample_stock_data, create_sample_portfolio
from portfolio_analysis import PortfolioAnalyzer
from risk_assessment import RiskAnalyzer
from predictive_modeling import FinancialPredictor


class TestDataIngestion:
    """Test data ingestion functionality."""

    def test_generate_sample_stock_data(self):
        """Test sample stock data generation."""
        symbols = ['AAPL', 'MSFT']
        data = generate_sample_stock_data(symbols, days=10)

        assert len(data) == 20  # 10 days * 2 symbols
        assert set(data['symbol'].unique()) == set(symbols)
        assert all(col in data.columns for col in ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])

    def test_create_sample_portfolio(self):
        """Test sample portfolio creation."""
        portfolio = create_sample_portfolio()

        assert len(portfolio) > 0
        required_cols = ['symbol', 'shares', 'purchase_price', 'current_price', 'sector']
        assert all(col in portfolio.columns for col in required_cols)
        assert (portfolio['current_price'] > 0).all()
        assert (portfolio['shares'] > 0).all()


class TestPortfolioAnalysis:
    """Test portfolio analysis functionality."""

    def setup_method(self):
        """Set up test data."""
        self.portfolio_data = pd.DataFrame({
            'symbol': ['AAPL', 'MSFT', 'GOOGL'],
            'shares': [100, 50, 25],
            'purchase_price': [150.0, 250.0, 2800.0],
            'current_price': [175.0, 280.0, 3100.0],
            'sector': ['Tech', 'Tech', 'Tech']
        })
        self.analyzer = PortfolioAnalyzer(self.portfolio_data)

    def test_portfolio_summary(self):
        """Test portfolio summary calculation."""
        summary = self.analyzer.get_portfolio_summary()

        assert 'total_value' in summary
        assert 'total_gain_loss' in summary
        assert summary['num_holdings'] == 3
        assert summary['total_value'] > 0

    def test_sector_allocation(self):
        """Test sector allocation calculation."""
        sector_alloc = self.analyzer.get_sector_allocation()

        assert not sector_alloc.empty
        assert 'allocation_pct' in sector_alloc.columns
        assert sector_alloc['allocation_pct'].sum() == pytest.approx(100.0, abs=1e-6)

    def test_weighted_metrics(self):
        """Test weighted metrics calculation."""
        weighted = self.analyzer.calculate_weighted_metrics()

        assert 'weighted_avg_return' in weighted
        assert 'portfolio_volatility' in weighted
        assert 'sharpe_ratio' in weighted


class TestRiskAssessment:
    """Test risk assessment functionality."""

    def setup_method(self):
        """Set up test data."""
        np.random.seed(42)
        self.returns = pd.Series(np.random.normal(0.001, 0.02, 100))
        self.analyzer = RiskAnalyzer()

    def test_historical_var(self):
        """Test historical VaR calculation."""
        var_result = self.analyzer.calculate_historical_var(self.returns, 1000000)

        assert 'var_amount' in var_result
        assert 'cvar_amount' in var_result
        assert var_result['confidence_level'] == 0.95
        assert var_result['var_amount'] > 0

    def test_parametric_var(self):
        """Test parametric VaR calculation."""
        var_result = self.analyzer.calculate_parametric_var(self.returns, 1000000)

        assert 'var_amount' in var_result
        assert 'mean_return' in var_result
        assert 'volatility' in var_result

    def test_sharpe_ratio(self):
        """Test Sharpe ratio calculation."""
        sharpe = self.analyzer.calculate_sharpe_ratio(self.returns)

        assert isinstance(sharpe, float)
        # Sharpe ratio should be reasonable for test data
        assert -5 <= sharpe <= 5

    def test_max_drawdown(self):
        """Test maximum drawdown calculation."""
        prices = pd.Series(np.cumprod(1 + self.returns))
        max_dd = self.analyzer.calculate_max_drawdown(prices)

        assert 'max_drawdown' in max_dd
        assert 0 <= max_dd['max_drawdown'] <= 1


class TestPredictiveModeling:
    """Test predictive modeling functionality."""

    def setup_method(self):
        """Set up test data."""
        np.random.seed(42)
        n_samples = 100
        n_features = 5

        # Generate sample data
        X = pd.DataFrame(np.random.randn(n_samples, n_features),
                        columns=[f'feature_{i}' for i in range(n_features)])
        y = pd.Series(X.sum(axis=1) + np.random.randn(n_samples) * 0.1)

        self.X = X
        self.y = y

    def test_financial_predictor_initialization(self):
        """Test predictor initialization."""
        predictor = FinancialPredictor('linear')
        assert predictor.model_type == 'linear'
        assert predictor.model is None

    def test_model_training(self):
        """Test model training."""
        predictor = FinancialPredictor('linear')
        X_train, X_test, y_train, y_test = predictor.train_test_split_temporal(self.X, self.y)

        train_metrics = predictor.fit(X_train, y_train)

        assert 'model_type' in train_metrics
        assert train_metrics['model_type'] == 'linear'
        assert predictor.model is not None

    def test_model_prediction(self):
        """Test model prediction."""
        predictor = FinancialPredictor('linear')
        X_train, X_test, y_train, y_test = predictor.train_test_split_temporal(self.X, self.y)

        predictor.fit(X_train, y_train)
        predictions = predictor.predict(X_test)

        assert len(predictions) == len(X_test)
        assert isinstance(predictions, np.ndarray)

    def test_model_evaluation(self):
        """Test model evaluation."""
        predictor = FinancialPredictor('linear')
        X_train, X_test, y_train, y_test = predictor.train_test_split_temporal(self.X, self.y)

        predictor.fit(X_train, y_train)
        eval_metrics = predictor.evaluate(X_test, y_test)

        required_metrics = ['mse', 'mae', 'r2', 'rmse']
        assert all(metric in eval_metrics for metric in required_metrics)
        assert all(isinstance(eval_metrics[metric], (int, float)) for metric in required_metrics)


class TestIntegration:
    """Integration tests for the complete system."""

    @patch('data_ingestion.load_stock_data')
    @patch('data_ingestion.create_sample_portfolio')
    def test_system_initialization(self, mock_portfolio, mock_market_data):
        """Test system initialization with mocked data."""
        from main import WealthManagementSystem

        # Mock return values
        mock_portfolio.return_value = pd.DataFrame({
            'symbol': ['AAPL'],
            'shares': [100],
            'purchase_price': [150.0],
            'current_price': [175.0],
            'sector': ['Tech']
        })

        mock_market_data.return_value = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=10),
            'close': np.random.randn(10) + 100
        })

        wms = WealthManagementSystem()
        wms.initialize_system()

        assert wms.portfolio_data is not None
        assert wms.market_data is not None
        assert wms.portfolio_analyzer is not None
        assert wms.risk_analyzer is not None


if __name__ == "__main__":
    pytest.main([__file__])