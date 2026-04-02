"""
Data Ingestion Module for Wealth Management POC

Handles loading and preprocessing of financial data including stock prices,
portfolio compositions, and market data.

Python Version: 3.9.6
Dependencies: pandas 1.5+, numpy 1.21+, yfinance 0.2+ (optional)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import warnings

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    warnings.warn("yfinance not available. Using sample data only.")


def generate_sample_stock_data(symbols: List[str], days: int = 252) -> pd.DataFrame:
    """
    Generate sample stock price data for demonstration.

    Args:
        symbols: List of stock symbols
        days: Number of trading days to generate

    Returns:
        DataFrame with OHLCV data for each symbol
    """
    end_date = datetime.now()
    dates = pd.bdate_range(end=end_date, periods=days)  # Exactly 'days' business days

    data = []
    np.random.seed(42)  # For reproducible results

    for symbol in symbols:
        # Generate realistic price movements
        base_price = np.random.uniform(50, 500)
        daily_returns = np.random.normal(0.0005, 0.02, len(dates))
        prices = base_price * np.exp(np.cumsum(daily_returns))

        # Generate OHLCV data
        for i, date in enumerate(dates):
            price = prices[i]
            volatility = 0.02
            high = price * (1 + np.random.uniform(0, volatility))
            low = price * (1 - np.random.uniform(0, volatility))
            open_price = price * (1 + np.random.normal(0, volatility/2))
            close = price
            volume = np.random.randint(100000, 10000000)

            data.append({
                'symbol': symbol,
                'date': date,
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': volume
            })

    return pd.DataFrame(data)


def load_stock_data(symbols: List[str], period: str = "1y") -> pd.DataFrame:
    """
    Load stock data from Yahoo Finance or generate sample data.

    Args:
        symbols: List of stock symbols (e.g., ['AAPL', 'GOOGL'])
        period: Time period for data ('1y', '2y', etc.)

    Returns:
        DataFrame with stock data
    """
    if YFINANCE_AVAILABLE:
        try:
            data = yf.download(symbols, period=period, group_by='ticker')
            # Flatten multi-index columns
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = ['_'.join(col).strip() for col in data.columns]
            return data.reset_index()
        except Exception as e:
            warnings.warn(f"Failed to download data: {e}. Using sample data.")
            return generate_sample_stock_data(symbols)
    else:
        return generate_sample_stock_data(symbols)


def create_sample_portfolio() -> pd.DataFrame:
    """
    Create a sample investment portfolio.

    Returns:
        DataFrame with portfolio holdings
    """
    portfolio = pd.DataFrame({
        'symbol': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA'],
        'shares': [100, 50, 25, 30, 20, 40],
        'purchase_price': [150.0, 250.0, 2800.0, 3200.0, 800.0, 400.0],
        'sector': ['Technology', 'Technology', 'Technology', 'Consumer', 'Automotive', 'Technology']
    })

    # Add current prices (simulated)
    np.random.seed(42)
    portfolio['current_price'] = portfolio['purchase_price'] * np.random.uniform(0.8, 1.5, len(portfolio))
    portfolio['current_price'] = portfolio['current_price'].round(2)

    return portfolio


def example() -> None:
    """Example usage of data ingestion module."""
    print("=== Data Ingestion Example ===")

    # Load stock data
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    stock_data = load_stock_data(symbols, period="6mo")
    print(f"Loaded {len(stock_data)} stock records")
    print(stock_data.head())

    # Create sample portfolio
    portfolio = create_sample_portfolio()
    print(f"\nSample Portfolio ({len(portfolio)} holdings):")
    print(portfolio)

    # Calculate basic portfolio value
    total_value = (portfolio['shares'] * portfolio['current_price']).sum()
    print(".2f")


if __name__ == "__main__":
    example()