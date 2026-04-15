# Customer 360 ETL - PySpark Implementation

This project contains a PySpark implementation of an Informatica ETL mapping that processes customer transaction data to generate a 360-degree customer view with aggregated metrics.

## Overview

The ETL pipeline converts the Informatica mapping `M_BFSI_CUSTOMER_360` into PySpark DataFrame operations, implementing the following transformations:

1. **Source Qualifier**: Incremental data extraction based on `LAST_UPDATED_TS`
2. **Expression**: Derived fields calculation (`TXN_FLAG`, `VALID_TXN`)
3. **Router**: Data validation and filtering
4. **Aggregator**: Transaction aggregation by customer
5. **Update Strategy**: SCD Type 2 logic (prepared for implementation)
6. **Target**: Load aggregated data to target table

## Architecture

### Source Schema
```sql
CUSTOMER_ID: INTEGER (NOT NULL)
ACCOUNT_ID: INTEGER
TXN_AMOUNT: DOUBLE
TXN_DATE: DATE
LAST_UPDATED_TS: TIMESTAMP
```

### Target Schema
```sql
CUSTOMER_ID: INTEGER (NOT NULL)
TOTAL_TXN: DOUBLE
AVG_TXN: DOUBLE
MAX_TXN: DOUBLE
LOAD_DT: DATE
```

## Features

### Production-Ready Implementation
- **Comprehensive Logging**: File and console logging with different log levels
- **Error Handling**: Try-catch blocks with proper exception handling
- **Schema Validation**: Strict schema enforcement for data quality
- **Performance Optimization**: Adaptive query execution, partitioning, and caching
- **Configuration Management**: Externalized configuration for easy deployment
- **Modular Design**: Object-oriented architecture for maintainability

### Data Quality Checks
- Null value validation for critical fields
- Data type validation
- Business rule validation (high-value transaction flagging)
- Record count validation and auditing

### Coding Standards
- PEP 8 compliant code style
- Comprehensive docstrings
- Type hints where applicable
- Unit test coverage
- Modular function design

## Installation

1. Ensure Java 17+ is installed:
   ```bash
   brew install openjdk@17
   export JAVA_HOME=/opt/homebrew/opt/openjdk@17
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Running the ETL Pipeline

```bash
# Activate virtual environment
source .venv/bin/activate

# Set Java environment
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

# Run the ETL job
python InformaticToPyspark/customer_360_etl.py
```

### Configuration

The ETL pipeline is configurable through the `config` dictionary in the main function:

```python
config = {
    'source_path': 'path/to/source/data.csv',
    'target_path': 'path/to/target/directory',
    'last_successful_run': '2023-01-01 00:00:00',
    'high_txn_threshold': 100000.0,
    'partition_by': ['CUSTOMER_ID'],
    'repartition_count': 4
}
```

### Running Tests

```bash
python -m pytest InformaticToPyspark/test_customer_360_etl.py -v
```

## Sample Data

The `sample_src_customer_txn.csv` file contains sample transaction data:

- 9 total records
- 1 invalid record (null TXN_AMOUNT)
- 4 unique customers
- Date range: 2023-01-05 to 2023-03-25

## Output

The ETL produces aggregated customer metrics:

| CUSTOMER_ID | TOTAL_TXN | AVG_TXN | MAX_TXN | LOAD_DT |
|-------------|-----------|---------|---------|---------|
| 1 | 245000.0 | 81666.67 | 120000.0 | 2026-04-15 |
| 2 | 105000.0 | 52500.0 | 80000.0 | 2026-04-15 |
| 3 | 245000.0 | 122500.0 | 150000.0 | 2026-04-15 |
| 4 | 30000.0 | 30000.0 | 30000.0 | 2026-04-15 |

## Business Logic

### Transaction Flagging
- `HIGH`: Transactions > $100,000
- `NORMAL`: Transactions ≤ $100,000

### Data Validation
- Records with null `TXN_AMOUNT` are rejected
- Invalid records are logged for audit purposes

### Aggregation
- Total transaction amount per customer
- Average transaction amount per customer
- Maximum transaction amount per customer

## Logging

Logs are written to:
- Console (INFO level and above)
- File: `customer_360_etl.log` (all levels)

Log entries include:
- Pipeline start/end timestamps
- Record counts at each stage
- Error details and stack traces
- Performance metrics

## Performance Considerations

### Optimizations Implemented
- **Adaptive Query Execution**: Dynamic optimization based on runtime statistics
- **Partitioning**: Data partitioning by CUSTOMER_ID for efficient aggregation
- **Repartitioning**: Optimal partition count for write operations
- **Schema Enforcement**: Early schema validation to avoid runtime issues
- **Caching**: Intelligent caching of intermediate results

### Scalability
- Designed to handle large datasets through Spark's distributed processing
- Configurable partitioning for different data volumes
- Memory-efficient processing with lazy evaluation

## Error Handling

### Exception Types Handled
- File I/O errors
- Schema validation errors
- Data processing errors
- Spark runtime errors

### Recovery Mechanisms
- Graceful shutdown on critical errors
- Detailed error logging for troubleshooting
- Partial failure handling with transaction boundaries

## Testing

### Test Coverage
- Unit tests for all major functions
- Integration test for complete ETL pipeline
- Schema validation tests
- Error condition tests
- Performance validation tests

### Test Data
- Synthetic test data with known edge cases
- Invalid data scenarios
- Boundary condition testing

## Deployment

### Production Deployment Checklist
- [ ] Java 17+ installed and configured
- [ ] PySpark dependencies installed
- [ ] Source and target paths configured
- [ ] Log directory permissions set
- [ ] Monitoring and alerting configured
- [ ] Backup and recovery procedures documented

### Monitoring
- Log file monitoring for errors
- Performance metrics collection
- Data quality dashboards
- Alerting for pipeline failures

## Future Enhancements

### Planned Features
- SCD Type 2 implementation for customer dimension
- Dynamic configuration from external files
- REST API for pipeline triggering
- Real-time streaming processing
- Advanced data quality rules
- Performance monitoring dashboard

### Extensibility
- Plugin architecture for custom transformations
- Configuration-driven pipeline orchestration
- Support for multiple source/target systems
- Cloud-native deployment support

## Contributing

1. Follow PEP 8 coding standards
2. Add unit tests for new features
3. Update documentation for API changes
4. Ensure backward compatibility
5. Run full test suite before committing

## License

This project is licensed under the MIT License - see the LICENSE file for details.