# Banking Domain Python Learning Project

This project demonstrates modular Python code covering:
- Python basics (`basic` module)
- Pandas data processing (`pandas_module` module)
- Pandas + Polars data processing (`pandas_polars_module` module)
- Math-heavy calculations (`math_libs` module)
- Statistics (`stats` module)
- PySpark batch processing (`pyspark_module` module)
- Orchestration patterns (`orchestration_module` module)
- LLM + RAG pipelines (`llm_module` module)
- Advanced AI architecture (`advanced_ai_module` module)
- AI Data Architect project (`ai_data_architect_project`)
- Data Engineering Architect project (`data_engineering_architect_project`)

Each module includes banking domain examples (customer account, transactions, risk, and portfolio computations). Also includes strategic path modules toward AI Data Architect (LangGraph, Ray, vector databases, RAG + Spark + LLM).
## Run module-wise

Use the top-level runner:

```bash
python run_module.py --module basic
python run_module.py --module pandas_module
python run_module.py --module math_libs
python run_module.py --module stats
python run_module.py --module pyspark_module
```

## Requirements

- Python 3.8+
- pandas
- numpy
- scipy
- statsmodels
- pyspark (optional)

### Java/PySpark compatibility (important)
- If your system has Java 1.8 (like `1.8.0_481`), use:
  - `pip install pyspark==2.4.8` or `pip install pyspark==3.0.3`
- If your system has Java 11, use:
  - `pip install pyspark>=3.1.0,<3.5`
- If your system has Java 17+, use:
  - `pip install pyspark>=3.5.0`

```bash
pip install pandas numpy scipy statsmodels
# then install Spark version matching your Java
pip install pyspark==2.4.8
```

