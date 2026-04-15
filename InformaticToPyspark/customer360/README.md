# Customer 360 ETL Pipeline

This project implements a PySpark ETL pipeline that processes customer transaction data to create a 360-degree customer view with aggregated transaction metrics.

## Overview

The ETL pipeline reads incremental customer transaction data, validates records, performs aggregations, and writes the results to a target table. It implements the logic from the Informatica `M_BFSI_CUSTOMER_360` mapping.

## Features

- **Data Validation**: Validates transaction amounts and filters out invalid records
- **Derived Fields**: Adds calculated fields like transaction flags and validation indicators
- **Aggregation**: Groups transactions by customer and calculates summary metrics
- **Schema Enforcement**: Ensures data conforms to target schema requirements
- **Production Ready**: Includes comprehensive logging, error handling, and performance optimizations

## Data Flow

1. **Source**: `sample_src_customer_txn.csv` - Incremental customer transaction data
2. **Validation**: Filter records with valid transaction amounts
3. **Transformation**: Add derived fields and perform aggregations
4. **Target**: `tgt_customer_agg` - Aggregated customer transaction data in Parquet format

## Schema

### Source Schema
- `CUSTOMER_ID` (Integer)
- `ACCOUNT_ID` (Integer)
- `TXN_AMOUNT` (Double)
- `TXN_DATE` (Date)
- `LAST_UPDATED_TS` (Timestamp)
- `TXN_FLAG` (String)

### Target Schema
- `CUSTOMER_ID` (Integer)
- `TOTAL_TXN_AMOUNT` (Double)
- `TXN_COUNT` (Integer)
- `AVG_TXN_AMOUNT` (Double)
- `LAST_TXN_DATE` (Date)
- `VALID_TXN_COUNT` (Integer)

## Configuration

The pipeline uses the following default configuration:
- Source path: `sample_src_customer_txn.csv`
- Target path: `tgt_customer_agg`
- Repartition count: 4
- Batch size: 10000

## Usage

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
python customer_360_etl.py
```

## Dependencies

- PySpark 3.x
- Python 3.9+
- Java 17 (OpenJDK)

## Logging

Logs are written to `customer_360_etl.log` with INFO level messages tracking each step of the ETL process.

## Testing

Run unit tests with:
```bash
pytest test_customer_360_etl.py
```

## Production Considerations

- Configurable partitioning for large datasets
- Schema validation before writing
- Comprehensive error handling and logging
- Performance optimizations with adaptive query execution</content>
<parameter name="filePath">/Users/sagarshingare/agent_pythonCode/InformaticToPyspark/customer360/README.md