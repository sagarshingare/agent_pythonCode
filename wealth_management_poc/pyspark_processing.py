"""
PySpark Data Processing Module for Wealth Management

Handles large-scale data processing for portfolio analytics,
risk modeling, and market data analysis using PySpark.

Python Version: 3.9.6
Dependencies: pyspark 3.5+, pandas 1.5+
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, stddev, count, sum as spark_sum
from pyspark.sql.functions import lag, lead, window, expr
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from typing import Dict, List, Optional, Tuple
import os
import pandas as pd
import numpy as np
import warnings

try:
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    warnings.warn("PySpark not available. This module requires PySpark installation.")


def _get_java_major() -> Optional[int]:
    """Return the major Java version (e.g., 8, 11, 17) or None if unavailable."""
    import subprocess
    import re

    java_home = os.environ.get("JAVA_HOME")
    java_bin = os.path.join(java_home, "bin", "java") if java_home else "java"

    try:
        output = subprocess.check_output([java_bin, "-version"], stderr=subprocess.STDOUT)
        output = output.decode("utf-8")
        match = re.search(r"version \"(\d+)(?:\.(\d+))?", output)
        if not match:
            return None

        major = int(match.group(1))
        if major == 1 and match.group(2) is not None:
            major = int(match.group(2))

        return major
    except Exception:
        return None


def _get_pyspark_major_minor() -> Optional[Tuple[int,int]]:
    """Return the major and minor version of installed PySpark, or None if not installed."""
    try:
        import pyspark

        v = pyspark.__version__.split('.')
        return int(v[0]), int(v[1])
    except Exception:
        return None


def _check_spark_java_compatibility() -> bool:
    """Check if Java + installed PySpark versions are compatible."""
    java_major = _get_java_major()
    pyspark_version = _get_pyspark_major_minor()

    if java_major is None or pyspark_version is None:
        return False

    pyspark_major, pyspark_minor = pyspark_version

    # PySpark 3.1+ requires Java 11+ (Java 17 recommended)
    if pyspark_major >= 3 and pyspark_minor >= 1:
        return java_major >= 11

    # PySpark 3.0.x supports Java 8 and 11
    if pyspark_major == 3 and pyspark_minor == 0:
        return java_major >= 8

    # PySpark 2.4.x supports Java 8
    if pyspark_major == 2:
        return java_major >= 8

    return False


def get_spark_session(app_name: str = "WealthManagementPOC") -> Optional[SparkSession]:
    """
    Create or get a Spark session.

    Args:
        app_name: Name for the Spark application

    Returns:
        SparkSession or None if unavailable
    """
    if not PYSPARK_AVAILABLE:
        warnings.warn("PySpark is not installed. PySpark modules are disabled.")
        return None

    java_major = _get_java_major()
    pyspark_compatible = _check_spark_java_compatibility()

    if java_major is None:
        warnings.warn("Unable to detect Java version; PySpark will be skipped.")
        return None

    if not pyspark_compatible:
        warnings.warn(
            f"Detected Java {java_major}; PySpark/install version compatibility check failed. "
            "For Java 8, use pyspark==2.4.8 or pyspark==3.0.3. For Java 11+, use pyspark>=3.1.0. "
            "For Java 17+, use pyspark>=3.5.0. PySpark features will be skipped."
        )
        return None

    try:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

    except Exception as exc:
        warnings.warn(f"Failed to create Spark session: {exc}. PySpark features will be skipped.")
        return None


def create_portfolio_schema() -> StructType:
    """
    Define schema for portfolio data.

    Returns:
        StructType for portfolio DataFrame
    """
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("shares", DoubleType(), True),
        StructField("purchase_price", DoubleType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("sector", StringType(), True)
    ])


def load_portfolio_data(spark: SparkSession, data_path: Optional[str] = None) -> DataFrame:
    """
    Load portfolio data into Spark DataFrame.

    Args:
        spark: SparkSession
        data_path: Path to data file (optional, uses sample data if None)

    Returns:
        Spark DataFrame with portfolio data
    """
    if data_path:
        return spark.read.csv(data_path, header=True, schema=create_portfolio_schema())
    else:
        # Create sample portfolio data
        sample_data = [
            ("AAPL", 100.0, 150.0, 175.0, "Technology"),
            ("MSFT", 50.0, 250.0, 280.0, "Technology"),
            ("GOOGL", 25.0, 2800.0, 3100.0, "Technology"),
            ("AMZN", 30.0, 3200.0, 3400.0, "Consumer"),
            ("TSLA", 20.0, 800.0, 950.0, "Automotive"),
            ("NVDA", 40.0, 400.0, 480.0, "Technology")
        ]
        return spark.createDataFrame(sample_data, schema=create_portfolio_schema())


def calculate_portfolio_metrics(spark_df: DataFrame) -> DataFrame:
    """
    Calculate portfolio-level metrics.

    Args:
        spark_df: Portfolio DataFrame

    Returns:
        DataFrame with portfolio metrics
    """
    return spark_df \
        .withColumn("position_value", col("shares") * col("current_price")) \
        .withColumn("gain_loss", (col("current_price") - col("purchase_price")) * col("shares")) \
        .withColumn("gain_loss_pct", (col("current_price") - col("purchase_price")) / col("purchase_price") * 100)


def aggregate_by_sector(spark_df: DataFrame) -> DataFrame:
    """
    Aggregate portfolio metrics by sector.

    Args:
        spark_df: Portfolio DataFrame with metrics

    Returns:
        DataFrame aggregated by sector
    """
    return spark_df.groupBy("sector") \
        .agg(
            spark_sum("position_value").alias("total_value"),
            spark_sum("gain_loss").alias("total_gain_loss"),
            avg("gain_loss_pct").alias("avg_gain_loss_pct"),
            count("symbol").alias("num_holdings")
        )


def process_market_data(spark: SparkSession, pandas_df: pd.DataFrame) -> DataFrame:
    """
    Process market data with Spark for time series analysis.

    Args:
        spark: SparkSession
        pandas_df: Pandas DataFrame with market data

    Returns:
        Spark DataFrame with processed market data
    """
    # Convert pandas to Spark
    spark_df = spark.createDataFrame(pandas_df)

    # Add technical indicators
    window_spec = Window.partitionBy("symbol").orderBy("date")

    return spark_df \
        .withColumn("daily_return", (col("close") - lag("close", 1).over(window_spec)) / lag("close", 1).over(window_spec)) \
        .withColumn("ma_20", avg("close").over(window_spec.rowsBetween(-19, 0))) \
        .withColumn("volatility_20", stddev("daily_return").over(window_spec.rowsBetween(-19, 0)))


def build_prediction_model(spark_df: DataFrame, target_col: str = "close",
                          feature_cols: List[str] = None) -> Dict:
    """
    Build a simple linear regression model for price prediction.

    Args:
        spark_df: Training data DataFrame
        target_col: Target column name
        feature_cols: List of feature column names

    Returns:
        Dictionary with model and metrics
    """
    if feature_cols is None:
        feature_cols = ["open", "high", "low", "volume"]

    # Prepare features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    assembled_df = assembler.transform(spark_df.dropna())

    # Split data
    train_df, test_df = assembled_df.randomSplit([0.8, 0.2], seed=42)

    # Train model
    lr = LinearRegression(featuresCol="features", labelCol=target_col)
    model = lr.fit(train_df)

    # Make predictions
    predictions = model.transform(test_df)

    # Evaluate
    evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction",
                                   metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    evaluator_r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction",
                                      metricName="r2")
    r2 = evaluator_r2.evaluate(predictions)

    return {
        "model": model,
        "rmse": rmse,
        "r2": r2,
        "feature_importance": dict(zip(feature_cols, model.coefficients))
    }


def example() -> None:
    """Example usage of PySpark data processing module."""
    if not PYSPARK_AVAILABLE:
        print("PySpark not available. Skipping example.")
        return

    print("=== PySpark Data Processing Example ===")

    # Initialize Spark
    spark = get_spark_session()
    if not spark:
        return

    try:
        # Load portfolio data
        portfolio_df = load_portfolio_data(spark)
        print(f"Loaded portfolio with {portfolio_df.count()} holdings")

        # Calculate metrics
        portfolio_metrics = calculate_portfolio_metrics(portfolio_df)
        print("\nPortfolio Metrics:")
        portfolio_metrics.select("symbol", "position_value", "gain_loss", "gain_loss_pct").show()

        # Aggregate by sector
        sector_agg = aggregate_by_sector(portfolio_metrics)
        print("\nSector Aggregation:")
        sector_agg.show()

        # Calculate total portfolio value
        total_value = portfolio_metrics.agg(spark_sum("position_value")).collect()[0][0]
        print(".2f")

        print("\n=== Market Data Processing ===")
        # Create sample market data
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        market_data = pd.DataFrame({
            'symbol': ['AAPL'] * 100,
            'date': dates,
            'open': np.random.uniform(150, 200, 100),
            'high': np.random.uniform(155, 205, 100),
            'low': np.random.uniform(145, 195, 100),
            'close': np.random.uniform(150, 200, 100),
            'volume': np.random.randint(1000000, 10000000, 100)
        })

        processed_market = process_market_data(spark, market_data)
        print("Processed market data with technical indicators:")
        processed_market.select("symbol", "date", "close", "daily_return", "ma_20").show(10)

    finally:
        spark.stop()


if __name__ == "__main__":
    example()