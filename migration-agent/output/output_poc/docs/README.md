# Informatica Migration Documentation

Auto-generated documentation for Informatica to PySpark migration.

## Summary

- **Total Mappings:** 1
- **Generated Date:** 2024-01-01
- **Status:** Complete

## Mappings

1. [m_comprehensive_poc](m_comprehensive_poc_doc.md) - Comprehensive POC mapping with all transformation types

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
