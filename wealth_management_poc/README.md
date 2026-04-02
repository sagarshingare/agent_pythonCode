# Wealth Management Proof-of-Concept (POC)

A comprehensive Python-based wealth management system demonstrating end-to-end financial analysis, risk assessment, and predictive modeling capabilities.

## Overview

This POC integrates multiple aspects of wealth management:

- **Data Ingestion**: Load and process financial market data
- **Portfolio Analysis**: Comprehensive portfolio performance and allocation analysis
- **Risk Assessment**: Multiple Value at Risk (VaR) calculations and risk metrics
- **Predictive Modeling**: Machine learning models for price forecasting and portfolio optimization
- **Big Data Processing**: PySpark integration for scalable data processing

## Python Version and Dependencies

- **Python Version**: 3.9.6
- **Environment**: Virtual environment recommended

### Core Dependencies
- pandas 1.5+
- numpy 1.21+
- scipy 1.7+
- statsmodels 0.13+
- scikit-learn 1.0+

### Optional Dependencies
- pyspark 3.5+ (for big data processing)
- tensorflow 2.8+ (for neural network models)
- matplotlib 3.5+ (for visualizations)
- yfinance 0.2+ (for real market data)

## Installation

1. Create and activate virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. For optional PySpark support:
- If Java 17+: install latest PySpark:
  ```bash
  pip install pyspark>=3.5.0
  ```
- If Java 11: install a compatible PySpark version:
  ```bash
  pip install pyspark>=3.1.0,<3.5
  ```
- If Java 8 (your case): install PySpark 2.4.8 or 3.0.3:
  ```bash
  pip install pyspark==2.4.8
  # or
  pip install pyspark==3.0.3
  ```

If you still get this error in Spark startup:
`UnsupportedClassVersionError: class file version 61.0, this version of the Java Runtime only recognizes class file versions up to 52.0`,
then either upgrade Java or use the Java-8-compatible PySpark versions above.

## Usage

### Run Full Analysis
```bash
python main.py
```

This will execute the complete wealth management analysis pipeline including:
- Portfolio performance analysis
- Risk assessment with multiple VaR calculations
- Predictive modeling for price forecasting
- Portfolio optimization
- Big data processing with PySpark (if available)

### Individual Module Usage

```python
from wealth_management_poc import *

# Portfolio Analysis
from portfolio_analysis import PortfolioAnalyzer
analyzer = PortfolioAnalyzer(portfolio_df)
summary = analyzer.get_portfolio_summary()

# Risk Assessment
from risk_assessment import RiskAnalyzer
risk_analyzer = RiskAnalyzer()
var = risk_analyzer.calculate_historical_var(returns)

# Predictive Modeling
from predictive_modeling import FinancialPredictor
predictor = FinancialPredictor('rf')
predictor.fit(X_train, y_train)
predictions = predictor.predict(X_test)
```

## Project Structure

```
wealth_management_poc/
├── __init__.py              # Package initialization
├── main.py                  # Main orchestration module
├── data_ingestion.py        # Data loading and preprocessing
├── portfolio_analysis.py    # Portfolio performance analysis
├── risk_assessment.py       # Risk metrics and VaR calculations
├── predictive_modeling.py   # ML models for predictions
├── pyspark_processing.py    # Big data processing with Spark
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Key Features

### Portfolio Analysis
- Total portfolio valuation and performance
- Sector allocation analysis
- Performance distribution statistics
- Investment recommendations

### Risk Assessment
- Historical Value at Risk (VaR)
- Parametric VaR (Normal distribution)
- Monte Carlo VaR simulation
- Sharpe and Sortino ratios
- Maximum drawdown analysis

### Predictive Modeling
- Multiple ML algorithms (Linear, Ridge, Random Forest, GBM)
- LSTM neural networks (with TensorFlow)
- Feature engineering with lagged variables
- Model evaluation metrics
- Portfolio optimization using mean-variance analysis

### Big Data Processing
- PySpark DataFrame operations
- Distributed portfolio analytics
- Large-scale data aggregation
- Performance metrics calculation

## Best Practices Implemented

- **PEP-8 Compliance**: Clean, readable code following Python standards
- **Type Hints**: Full type annotations for better code documentation
- **Error Handling**: Graceful degradation when optional dependencies are missing
- **Modular Design**: Separated concerns with focused modules
- **Comprehensive Docstrings**: Detailed function and class documentation
- **Version Specifications**: Explicit library version requirements

## Testing

Run tests with pytest:
```bash
pytest
```

## Development

### Code Quality
```bash
# Format code
black .

# Lint code
flake8 .

# Type checking
mypy .
```

### Adding New Features
1. Create new module in appropriate file
2. Add imports to `main.py` if needed
3. Update `requirements.txt` for new dependencies
4. Add comprehensive docstrings and type hints
5. Test thoroughly with sample data

## Example Output

```
Wealth Management Proof-of-Concept System
==================================================

Initializing Wealth Management System...
Loading sample data...
Loaded portfolio with 6 holdings
Loaded market data for 6 symbols
Spark session initialized.
System initialization complete.

=== Portfolio Analysis ===
Portfolio Summary:
  Total Value: $1,234,567.89
  Total Cost Basis: $1,123,456.78
  Total Gain/Loss: $111,111.11
  Total Return: 9.90%
  Number of Holdings: 6

... [additional analysis output]
```

## License

This project is for educational and demonstration purposes.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with proper documentation
4. Add tests for new functionality
5. Submit a pull request

## Future Enhancements

- Real-time data integration
- Web-based dashboard
- Advanced AI models (Reinforcement Learning for trading)
- Multi-asset class support
- Performance attribution analysis
- Tax optimization features