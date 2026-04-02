"""Run Data Engineering Architect modules."""

from data_engineering_architect_project import pandas_etl, pyspark_etl, orchestration_etl


def main() -> int:
    print('=== DATA ENGINEERING ARCHITECT RUNNER ===')
    pandas_etl.example()
    pyspark_etl.example()
    orchestration_etl.example()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
