# Informatica Migration Documentation

Auto-generated documentation for Informatica to PySpark migration.

## Summary

- **Total Mappings:** 3
- **Generated Date:** 2024-01-01
- **Status:** Complete

## Mappings

1. [m_COMPTIME_Build_Message_Counters](m_comptime_build_message_counters_doc.md) - This mapping gets the count of detail records on the CompTime file that was processed and loads it to the Counters Table. 
2. [m_COMPTIME_Load_COMP_TIME_DAILY_TBL](m_comptime_load_comp_time_daily_tbl_doc.md) - 
3. [m_COMPTIME_Current_Pay_Period](m_comptime_current_pay_period_doc.md) - This mapping returns the Current Pay Period from the Pay Period table.

## Generated Artifacts

- PySpark Code: `output/pyspark/main_job.py`
- Validation SQL: `output/sql/validation.sql`
- Airflow DAG: `output/dags/informatica_migration_dag.py`
- Sample Data: `data/generated_*.csv`

## Architecture

The migration uses a multi-agent architecture:

1. **SpecAgent** - Parses XML and builds canonical model
2. **CodeGenAgent** - Generates PySpark code
3. **ValidationAgent** - Creates validation SQL
4. **OptimizationAgent** - Optimizes code performance
5. **OrchestrationAgent** - Generates Airflow DAG
6. **DocumentationAgent** - Creates documentation
