"""Comprehensive learning and project guide module for banking learning project."""

from typing import Dict, List


def get_guide_sections() -> Dict[str, List[str]]:
    """Return the complete learning guide as structured sections."""
    return {
        "python_foundation": [
            "Use Python 3.11+ (typing, performance, modern syntax)",
            "Core syntax: control flow, functions, OOP (classes, inheritance, dunder methods)",
            "Typing: List, Dict, Optional, Union, TypeAlias, Protocol, TypedDict",
            "PEP-8 style + black + ruff/flake8 + docstrings (PEP-257)",
            "Testing with pytest + parametrize + fixtures",
            "CI pipeline: lint, mypy, pytest with GitHub Actions",
        ],
        "data_engineering": [
            "Pandas 1.5+ / 2.x: DataFrame ops, groupby, pivot, in/out, datetimes",
            "Polars 0.19+: lazy API, expressions, large dataset scaling",
            "PySpark 3.x: SparkSession, DataFrame API, sql, window functions, caching",
            "Storage: Parquet, Delta Lake, S3/MinIO, vector DB (FAISS/Chroma)",
            "Version notes: pandas 2.0 for new nullable arrays, pyspark 3.4 for Python 3.11 compatibility",
        ],
        "orchestration": [
            "Airflow 2.8+: DAGs, tasks, dependencies, retry, schedule, XCom",
            "Prefect 2.x: flows, tasks, deployments, state management",
            "Pipeline use-case: daily ETL, QA checks, model training, deployment",
        ],
        "ai_architecture": [
            "Transformers 4.30+: text-generation pipeline, tokenizer+model, fine-tuning",
            "OpenAI 0.30+: ChatCompletion, embeddings for RAG",
            "LangChain 0.0.3xx+: chains, retrievers, vector store connectors",
            "LlamaIndex 0.8+: document loaders, indexing for docs, queryables",
            "Ray 2.x: parallel/distributed jobs for embedding and scoring",
            "LangGraph concept: workflow agent orchestration (pipeline graph)",
        ],
        "end_to_end": [
            "ETL ingestion -> transform -> feature generation (pandas/pyspark)",
            "Feature store -> indexing (vector DB) -> RAG query layer",
            "Orchestration: Airflow/Prefect runs full pipeline and monitors run statuses",
            "Artifact packaging: Dockerfile, docker-compose, cloud deployment path",
            "Reliability: logging, tests, SLA alerts, error handling, retries",
        ],
    }


def print_guide() -> None:
    """Print the full guide in human-readable format."""
    sections = get_guide_sections()
    print("\n=== Banking AI + Data Engineering Learning Guide ===\n")
    for title, bullets in sections.items():
        print(f"-- {title.replace('_', ' ').title()} --")
        for bullet in bullets:
            print(f"  - {bullet}")
        print("\n")


def example() -> None:
    """Example function to display guide; included for module compatibility."""
    print_guide()
