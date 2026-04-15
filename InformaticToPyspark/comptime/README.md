# CompTime ETL Pipeline

This project implements a PySpark ETL pipeline that processes compensatory time data from flat files, validates records, loads to database tables, and generates counters and messages.

## Overview

The ETL pipeline implements three Informatica mappings:
1. `m_COMPTIME_Current_Pay_Period` - Gets current pay period information
2. `m_COMPTIME_Load_COMP_TIME_DAILY_TBL` - Loads comp time data with validation
3. `m_COMPTIME_Build_Message_Counters` - Counts records and builds messages

## Features

- **Pay Period Lookup**: Dynamically determines current pay period from reference data
- **Data Validation**: Validates SSN format and filters invalid records
- **Type Conversion**: Converts date strings to proper date types
- **Counter Generation**: Creates audit counters for processed records
- **Message Generation**: Produces email notifications with processing summaries
- **Production Ready**: Includes comprehensive logging, error handling, and schema validation

## Data Flow

1. **Pay Period Source**: `sample_pay_period.csv` - Reference pay period data
2. **CompTime Source**: `sample_comptime_data.csv` - Compensatory time transaction data
3. **Validation**: Filter records with valid SSN format
4. **Transformation**: Convert data types and add pay period fields
5. **Targets**:
   - `comp_time_daily_tbl` - Processed comp time data in Parquet format
   - `counter_tbl` - Audit counters in Parquet format
   - `comptime_message_file.txt` - Email message content
   - `comp_time_date_file.txt` - Current pay period identifier

## Schema

### CompTime Source Schema
- `SSN` (String)
- `NAME` (String)
- `CURRENT_ACCT` (String)
- `CURRENT_ORG` (String)
- `FLSA_STATUS` (String)
- `COMP_TIME_CUR_BAL` (Double)
- `COMP_TIME_YEAR_EARNED` (Integer)
- `PP_END_DATE` (String)
- `DAILY_DATE_EARNED` (String)
- `COMP_TIME_RATE` (Double)
- `COMP_TIME_HOURS` (Double)
- `COMP_TIME_UNDEF` (Double)

### COMP_TIME_DAILY_TBL Target Schema
- `PP_END_YEAR` (Integer)
- `PP_NUM` (Integer)
- `PP_YEAR_NUM` (Integer)
- `SSN` (String)
- `NAME` (String)
- `CURRENT_ACCT` (String)
- `CURRENT_ORG` (String)
- `FLSA_STATUS` (String)
- `COMP_TIME_CUR_BAL` (Double)
- `COMP_TIME_YEAR_EARNED` (Integer)
- `PP_END_DATE` (Date)
- `DAILY_DATE_EARNED` (Date)
- `COMP_TIME_RATE` (Double)
- `COMP_TIME_HOURS` (Double)
- `COMP_TIME_UNDEF` (Double)

## Configuration

The pipeline uses the following default configuration:
- CompTime source: `sample_comptime_data.csv`
- Pay period source: `sample_pay_period.csv`
- Comp time daily target: `comp_time_daily_tbl`
- Counter target: `counter_tbl`
- Message file: `comptime_message_file.txt`
- Date file: `comp_time_date_file.txt`
- Repartition count: 4
- Batch size: 10000

## Usage

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
python comptime_etl.py
```

## Dependencies

- PySpark 3.x
- Python 3.9+
- Java 17 (OpenJDK)

## Logging

Logs are written to `comptime_etl.log` with INFO level messages tracking each step of the ETL process.

## Testing

Run unit tests with:
```bash
pytest test_comptime_etl.py
```

## Production Considerations

- Dynamic pay period determination
- SSN validation for data quality
- Schema validation before writing
- Counter and message generation for auditing
- Configurable partitioning for performance
- Comprehensive error handling and logging</content>
<parameter name="filePath">/Users/sagarshingare/agent_pythonCode/InformaticToPyspark/comptime/README.md