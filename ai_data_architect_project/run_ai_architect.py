"""Run AI Data Architect modules."""

from ai_data_architect_project import llm_waas, vector_db, spark_rag


def main() -> int:
    print('=== AI DATA ARCHITECT RUNNER ===')
    llm_waas.example()
    vector_db.example()
    spark_rag.example()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
