"""
Predictive Modeling Module for Wealth Management

Implements machine learning models for financial predictions including
stock price forecasting, portfolio optimization, and risk prediction.

Python Version: 3.9.6
Dependencies: scikit-learn 1.0+, pandas 1.5+, numpy 1.21+, tensorflow 2.8+ (optional)
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import GridSearchCV
from typing import Dict, List, Tuple, Optional, Union
import warnings

try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    warnings.warn("TensorFlow not available. Neural network models will be disabled.")


class FinancialPredictor:
    """
    Class for building and evaluating financial prediction models.
    """

    def __init__(self, model_type: str = 'linear', random_state: int = 42):
        """
        Initialize the predictor.

        Args:
            model_type: Type of model ('linear', 'ridge', 'lasso', 'rf', 'gbm', 'lstm')
            random_state: Random state for reproducibility
        """
        self.model_type = model_type
        self.random_state = random_state
        self.model = None
        self.scaler = None
        self.feature_names = None

    def prepare_features(self, data: pd.DataFrame, target_col: str,
                        feature_cols: Optional[List[str]] = None,
                        lag_features: int = 5) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Prepare features for modeling including lagged variables.

        Args:
            data: Input DataFrame
            target_col: Target column name
            feature_cols: List of feature columns (optional)
            lag_features: Number of lag periods to create

        Returns:
            Tuple of (features DataFrame, target Series)
        """
        if feature_cols is None:
            feature_cols = [col for col in data.columns if col != target_col]

        df = data.copy()

        # Create lagged features
        for col in feature_cols + [target_col]:
            for lag in range(1, lag_features + 1):
                df[f'{col}_lag_{lag}'] = df[col].shift(lag)

        # Remove rows with NaN values
        df = df.dropna()

        # Separate features and target
        feature_cols_extended = [col for col in df.columns if col != target_col]
        X = df[feature_cols_extended]
        y = df[target_col]

        self.feature_names = feature_cols_extended

        return X, y

    def train_test_split_temporal(self, X: pd.DataFrame, y: pd.Series,
                                test_size: float = 0.2) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Split data temporally (respecting time order).

        Args:
            X: Features DataFrame
            y: Target Series
            test_size: Proportion for test set

        Returns:
            Tuple of (X_train, X_test, y_train, y_test)
        """
        split_idx = int(len(X) * (1 - test_size))

        X_train = X.iloc[:split_idx]
        X_test = X.iloc[split_idx:]
        y_train = y.iloc[:split_idx]
        y_test = y.iloc[split_idx:]

        return X_train, X_test, y_train, y_test

    def build_model(self) -> None:
        """Build the specified model."""
        if self.model_type == 'linear':
            self.model = LinearRegression()
        elif self.model_type == 'ridge':
            self.model = Ridge(alpha=1.0, random_state=self.random_state)
        elif self.model_type == 'lasso':
            self.model = Lasso(alpha=0.01, random_state=self.random_state)
        elif self.model_type == 'rf':
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=self.random_state
            )
        elif self.model_type == 'gbm':
            self.model = GradientBoostingRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=5,
                random_state=self.random_state
            )
        elif self.model_type == 'lstm':
            if not TENSORFLOW_AVAILABLE:
                raise ImportError("TensorFlow required for LSTM model")
            # LSTM will be built during fit
        else:
            raise ValueError(f"Unknown model type: {self.model_type}")

    def fit(self, X: pd.DataFrame, y: pd.Series) -> Dict:
        """
        Fit the model to the data.

        Args:
            X: Training features
            y: Training target

        Returns:
            Dictionary with training metrics
        """
        self.build_model()

        if self.model_type == 'lstm':
            return self._fit_lstm(X, y)
        else:
            return self._fit_traditional(X, y)

    def _fit_traditional(self, X: pd.DataFrame, y: pd.Series) -> Dict:
        """Fit traditional ML models."""
        # Scale features
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X)

        # Fit model
        self.model.fit(X_scaled, y)

        # Training predictions
        y_pred = self.model.predict(X_scaled)

        # Calculate metrics
        mse = mean_squared_error(y, y_pred)
        mae = mean_absolute_error(y, y_pred)
        r2 = r2_score(y, y_pred)

        return {
            'mse': round(mse, 4),
            'mae': round(mae, 4),
            'r2': round(r2, 4),
            'model_type': self.model_type
        }

    def _fit_lstm(self, X: pd.DataFrame, y: pd.Series) -> Dict:
        """Fit LSTM neural network."""
        # Prepare data for LSTM (3D format)
        X_scaled = StandardScaler().fit_transform(X)
        X_reshaped = X_scaled.reshape((X_scaled.shape[0], 1, X_scaled.shape[1]))

        # Build LSTM model
        self.model = Sequential([
            LSTM(50, activation='relu', input_shape=(1, X_scaled.shape[1])),
            Dropout(0.2),
            Dense(1)
        ])

        self.model.compile(optimizer='adam', loss='mse')

        # Fit model
        history = self.model.fit(
            X_reshaped, y.values,
            epochs=50,
            batch_size=32,
            verbose=0,
            validation_split=0.1
        )

        return {
            'final_loss': round(history.history['loss'][-1], 4),
            'final_val_loss': round(history.history['val_loss'][-1], 4),
            'model_type': 'lstm'
        }

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Make predictions with the trained model.

        Args:
            X: Input features

        Returns:
            Array of predictions
        """
        if self.model is None:
            raise ValueError("Model not trained. Call fit() first.")

        if self.model_type == 'lstm':
            X_scaled = StandardScaler().fit_transform(X)  # Note: using new scaler
            X_reshaped = X_scaled.reshape((X_scaled.shape[0], 1, X_scaled.shape[1]))
            return self.model.predict(X_reshaped, verbose=0).flatten()
        else:
            X_scaled = self.scaler.transform(X)
            return self.model.predict(X_scaled)

    def evaluate(self, X_test: pd.DataFrame, y_test: pd.Series) -> Dict:
        """
        Evaluate model performance on test data.

        Args:
            X_test: Test features
            y_test: Test target

        Returns:
            Dictionary with evaluation metrics
        """
        y_pred = self.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        # Directional accuracy (for financial predictions)
        actual_direction = np.sign(y_test.values[1:] - y_test.values[:-1])
        pred_direction = np.sign(y_pred[1:] - y_pred[:-1])
        directional_accuracy = np.mean(actual_direction == pred_direction)

        return {
            'mse': round(mse, 4),
            'mae': round(mae, 4),
            'r2': round(r2, 4),
            'directional_accuracy': round(directional_accuracy, 4),
            'rmse': round(np.sqrt(mse), 4)
        }

    def get_feature_importance(self) -> Optional[pd.DataFrame]:
        """
        Get feature importance if available.

        Returns:
            DataFrame with feature importance or None
        """
        if self.model is None:
            return None

        if hasattr(self.model, 'feature_importances_'):
            importance = self.model.feature_importances_
        elif hasattr(self.model, 'coef_'):
            importance = np.abs(self.model.coef_)
        else:
            return None

        if self.feature_names is None:
            return None

        return pd.DataFrame({
            'feature': self.feature_names,
            'importance': importance
        }).sort_values('importance', ascending=False)


class PortfolioOptimizer:
    """
    Class for portfolio optimization using mean-variance optimization.
    """

    def __init__(self, returns: pd.DataFrame, risk_free_rate: float = 0.02):
        """
        Initialize portfolio optimizer.

        Args:
            returns: DataFrame with asset returns
            risk_free_rate: Risk-free rate for Sharpe ratio
        """
        self.returns = returns
        self.risk_free_rate = risk_free_rate
        self.mean_returns = returns.mean()
        self.cov_matrix = returns.cov()

    def optimize_portfolio(self, target_return: Optional[float] = None,
                          num_portfolios: int = 10000) -> Dict:
        """
        Optimize portfolio using random portfolio generation.

        Args:
            target_return: Target portfolio return (optional)
            num_portfolios: Number of random portfolios to generate

        Returns:
            Dictionary with optimal portfolio weights and metrics
        """
        num_assets = len(self.mean_returns)
        results = np.zeros((3, num_portfolios))
        weights_record = []

        for i in range(num_portfolios):
            weights = np.random.random(num_assets)
            weights /= np.sum(weights)
            weights_record.append(weights)

            # Portfolio return and volatility
            portfolio_return = np.sum(weights * self.mean_returns)
            portfolio_volatility = np.sqrt(weights.T @ self.cov_matrix @ weights)

            # Sharpe ratio
            sharpe_ratio = (portfolio_return - self.risk_free_rate) / portfolio_volatility

            results[0, i] = portfolio_return
            results[1, i] = portfolio_volatility
            results[2, i] = sharpe_ratio

        # Find optimal portfolios
        max_sharpe_idx = np.argmax(results[2])
        min_vol_idx = np.argmin(results[1])

        if target_return:
            # Find portfolio with target return and minimum volatility
            target_idx = np.argmin(np.abs(results[0] - target_return))
        else:
            target_idx = max_sharpe_idx

        return {
            'max_sharpe_portfolio': {
                'weights': weights_record[max_sharpe_idx],
                'return': results[0, max_sharpe_idx],
                'volatility': results[1, max_sharpe_idx],
                'sharpe_ratio': results[2, max_sharpe_idx]
            },
            'min_volatility_portfolio': {
                'weights': weights_record[min_vol_idx],
                'return': results[0, min_vol_idx],
                'volatility': results[1, min_vol_idx],
                'sharpe_ratio': results[2, min_vol_idx]
            },
            'target_portfolio': {
                'weights': weights_record[target_idx],
                'return': results[0, target_idx],
                'volatility': results[1, target_idx],
                'sharpe_ratio': results[2, target_idx]
            } if target_return else None
        }


def generate_sample_price_data(num_days: int = 252) -> pd.DataFrame:
    """
    Generate sample price data for testing.

    Args:
        num_days: Number of trading days

    Returns:
        DataFrame with OHLCV data
    """
    np.random.seed(42)

    dates = pd.date_range('2023-01-01', periods=num_days, freq='D')

    # Base price with trend and volatility
    base_price = 100
    trend = 0.0002  # Slight upward trend
    volatility = 0.02

    prices = [base_price]
    for i in range(1, num_days):
        return_val = np.random.normal(trend, volatility)
        new_price = prices[-1] * (1 + return_val)
        prices.append(new_price)

    # Generate OHLCV
    data = []
    for i, price in enumerate(prices):
        vol = 0.015
        high = price * (1 + np.random.uniform(0, vol))
        low = price * (1 - np.random.uniform(0, vol))
        open_price = price * (1 + np.random.normal(0, vol/2))
        close = price
        volume = np.random.randint(100000, 1000000)

        data.append({
            'date': dates[i],
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': volume
        })

    return pd.DataFrame(data)


def example() -> None:
    """Example usage of predictive modeling module."""
    print("=== Predictive Modeling Example ===")

    # Generate sample data
    price_data = generate_sample_price_data(500)
    print(f"Generated {len(price_data)} days of price data")

    # Prepare features for prediction
    predictor = FinancialPredictor(model_type='rf')

    # Use close price as target, other features as predictors
    feature_cols = ['open', 'high', 'low', 'volume']
    X, y = predictor.prepare_features(price_data, 'close', feature_cols, lag_features=3)

    print(f"Prepared {X.shape[1]} features for {len(X)} samples")

    # Split data
    X_train, X_test, y_train, y_test = predictor.train_test_split_temporal(X, y)

    # Train model
    train_metrics = predictor.fit(X_train, y_train)
    print(f"\nTraining completed for {train_metrics['model_type']} model")
    print(f"Training R²: {train_metrics.get('r2', 'N/A')}")

    # Evaluate on test set
    test_metrics = predictor.evaluate(X_test, y_test)
    print("\nTest Performance:")
    print(f"  MSE: {test_metrics['mse']}")
    print(f"  MAE: {test_metrics['mae']}")
    print(f"  R²: {test_metrics['r2']}")
    print(f"  Directional Accuracy: {test_metrics['directional_accuracy']:.1%}")

    # Feature importance
    importance = predictor.get_feature_importance()
    if importance is not None:
        print("\nTop 5 Important Features:")
        print(importance.head())

    # Portfolio optimization example
    print("\n=== Portfolio Optimization ===")
    # Generate sample returns for multiple assets
    np.random.seed(42)
    asset_returns = pd.DataFrame({
        'Asset_1': np.random.normal(0.001, 0.02, 252),
        'Asset_2': np.random.normal(0.0008, 0.025, 252),
        'Asset_3': np.random.normal(0.0012, 0.018, 252),
        'Asset_4': np.random.normal(0.0005, 0.022, 252)
    })

    optimizer = PortfolioOptimizer(asset_returns)
    optimal_portfolios = optimizer.optimize_portfolio()

    print("Maximum Sharpe Ratio Portfolio:")
    max_sharpe = optimal_portfolios['max_sharpe_portfolio']
    for i, weight in enumerate(max_sharpe['weights']):
        print(".1%")
    print(".2%")
    print(".2%")
    print(".2f")


if __name__ == "__main__":
    example()