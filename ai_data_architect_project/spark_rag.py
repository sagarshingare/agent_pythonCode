"""Spark + RAG pipeline template for AI Data Architect project."""

from typing import List, Dict

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None


def get_spark() -> 'SparkSession':
    if SparkSession is None:
        raise ImportError("pyspark not installed")
    return SparkSession.builder.appName("ai_data_architect").master("local[1]").getOrCreate()


def example() -> None:
    print("--- AI DATA ARCHITECT SPARK RAG ---")
    try:
        spark = get_spark()
        print("Spark session active", spark)
        spark.stop()
    except Exception as exc:
        print("Spark not available", exc)
