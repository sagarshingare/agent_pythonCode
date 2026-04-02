"""PySpark banking data pipeline examples with key DataFrame & ML helpers.

This module follows PySpark 3.x API conventions and includes function-level notes where
applicable for Spark version features.

Maybe not literally every Spark function (PySpark has hundreds), but this set includes
core row/column operations, SQL recipe, windowing, join, ML pipeline and commonly
used DataFrame transformations.
"""

from typing import List, Dict, Union, Optional


def get_spark_session(app_name: str = "banking_example", master: str = "local[1]", spark_conf: Optional[Dict[str, str]] = None):
    """Create or get SparkSession.

    Spark version: 3.0+ (round-tripped through 4.x)
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ImportError("PySpark is required for pyspark_module: pip install pyspark") from exc

    builder = SparkSession.builder.appName(app_name).master(master)
    if spark_conf:
        for k, v in spark_conf.items():
            builder = builder.config(k, v)
    return builder.getOrCreate()


def stop_spark_session(spark):
    """Stop SparkSession."""
    if spark is not None:
        spark.stop()


def create_dataframe(spark, data: List[Dict[str, Union[str, float, int]]]):
    """Create Spark DataFrame from a list of dictionary rows."""
    return spark.createDataFrame(data)


def select_columns(df, columns: List[str]):
    """Select a subset of columns (DataFrame.select)."""
    return df.select(*columns)


def filter_rows(df, predicate: str):
    """Filter rows with SQL-like predicate (DataFrame.filter).

    PySpark 3.0+ supports lambda style and SQL string filtering.
    """
    return df.filter(predicate)


def show_schema(df):
    """Print DataFrame schema. (DataFrame.printSchema in Spark 1.x+)."""
    df.printSchema()


def add_signed_amount(df, credit_col: str = "txn_type", amount_col: str = "amount"):
    """Add signed amount column, credit positive, debit negative (PySpark 3.x)."""
    from pyspark.sql.functions import col, when

    return df.withColumn(
        "signed_amount",
        when(col(credit_col) == "credit", col(amount_col)).otherwise(-col(amount_col)),
    )


def compute_net_balance_by_account(df, account_col: str = "account_id"):
    """Aggregate net balance by account via groupBy+agg."""
    from pyspark.sql.functions import sum as spark_sum

    return df.groupBy(account_col).agg(spark_sum("signed_amount").alias("net_balance")).orderBy(account_col)


def with_date_as_timestamp(df, date_col: str = "date", fmt: str = "yyyy-MM-dd"):
    """Cast date text to Timestamp (DataFrame.withColumn+to_timestamp)."""
    from pyspark.sql.functions import to_timestamp

    return df.withColumn(date_col, to_timestamp(date_col, fmt))


def compute_monthly_balance(df, date_col: str = "date"):
    """Compute year/month rolling net balance (PySpark 3.x window functions)."""
    from pyspark.sql.functions import col, date_trunc, sum as spark_sum

    return (
        df.withColumn(date_col, col(date_col).cast("timestamp"))
          .withColumn("month", date_trunc("month", col(date_col)))
          .groupBy("account_id", "month")
          .agg(spark_sum("signed_amount").alias("net_balance"))
          .orderBy("account_id", "month")
    )


def register_temp_view(df, view_name: str):
    """Register DataFrame to a temporary view for Spark SQL."""
    df.createOrReplaceTempView(view_name)


def execute_sql(spark, query: str):
    """Execute Spark SQL query via SparkSession.sql."""
    return spark.sql(query)


def join_accounts_transactions(accounts_df, transactions_df, how: str = "inner", key: str = "account_id"):
    """DataFrame join (inner, left, right, outer)."""
    return accounts_df.join(transactions_df, on=key, how=how)


def select_columns(df, columns: List[str]):
    """Wrapper for DataFrame.select (PySpark 1.x+)."""
    return df.select(*columns)


def order_by(df, *cols, ascending=True):
    """Wrapper for DataFrame.orderBy (PySpark 1.x+)."""
    return df.orderBy(*cols, ascending=ascending)


def group_by(df, *cols):
    """Wrapper for DataFrame.groupBy (PySpark 1.x+)."""
    return df.groupBy(*cols)


def with_column(df, col_name: str, expr):
    """Wrapper for DataFrame.withColumn (PySpark 1.x+)."""
    return df.withColumn(col_name, expr)


def drop_column(df, col_name: str):
    """Wrapper for DataFrame.drop (PySpark 1.x+)."""
    return df.drop(col_name)


def distinct_rows(df):
    """Wrapper for DataFrame.distinct (PySpark 1.x+)."""
    return df.distinct()


def limit_rows(df, n: int):
    """Wrapper for DataFrame.limit (PySpark 1.x+)."""
    return df.limit(n)


def repartition(df, num_partitions: int):
    """Wrapper for DataFrame.repartition (PySpark 1.3+)."""
    return df.repartition(num_partitions)


def cache_dataframe(df):
    """Wrapper for DataFrame.cache (PySpark 1.2+)."""
    return df.cache()


def uncache_dataframe(df):
    """Wrapper for DataFrame.unpersist (PySpark 1.2+)."""
    return df.unpersist()


def union_dataframes(df1, df2):
    """Wrapper for DataFrame.unionByName (PySpark 2.3+)."""
    return df1.unionByName(df2)


def add_column_merged_from_struct(df, struct_col, new_col_name):
    """Demonstrate use of DataFrame.selectExpr/withColumn on struct (PySpark 1.x+)."""
    from pyspark.sql.functions import col
    return df.withColumn(new_col_name, col(struct_col + ".*"))


def accumulate_rolling_balance(df, order_col: str = "date", partition_cols: Optional[List[str]] = None):
    """Rolling cumulative balance via Window functions (PySpark 3.x)."""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import sum as spark_sum, row_number, col

    if partition_cols is None:
        partition_cols = ["account_id"]

    win = Window.partitionBy(*partition_cols).orderBy(order_col).rowsBetween(Window.unboundedPreceding, 0)
    return df.withColumn("cumulative_balance", spark_sum("signed_amount").over(win)).withColumn("txn_order", row_number().over(win))


def split_once(df, input_col: str, output1: str, output2: str, delimiter: str = ","):
    """Split a string column into two columns using pyspark.sql.functions.split (3.1+)."""
    from pyspark.sql.functions import split

    split_col = split(df[input_col], delimiter)
    return df.withColumn(output1, split_col.getItem(0)).withColumn(output2, split_col.getItem(1))


def pivot_by_category(df, index_col: str, pivot_col: str, value_col: str):
    """Pivot table aggregation (DataFrame.groupBy.pivot, Spark 3.0+)."""
    from pyspark.sql.functions import sum as spark_sum

    return df.groupBy(index_col).pivot(pivot_col).agg(spark_sum(value_col))


def ml_pipeline_linear_regression(df, feature_cols: List[str], label_col: str = "label"):
    """Train Linear Regression using Spark ML pipeline. (requires ml package)

    - VectorAssembler: Spark 1.4+
    - LinearRegression: Spark 1.4+
    - Pipeline: Spark 2.0+
    """
    try:
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import LinearRegression
        from pyspark.ml import Pipeline
    except ImportError as exc:
        raise ImportError("PySpark ML is required: pip install pyspark") from exc

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol=label_col, maxIter=10)
    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(df)
    return model


def spark_ml_predict(model, df):
    """Predict using fitted pipeline model."""
    return model.transform(df)


def example() -> None:
    """Banking usage example that exercises module methods."""
    spark = get_spark_session("banking_example", "local[1]")
    try:
        txns = [
            {"txn_id": "P001", "account_id": "A001", "amount": 1000.0, "txn_type": "credit", "date": "2025-12-01"},
            {"txn_id": "P002", "account_id": "A001", "amount": 300.0, "txn_type": "debit", "date": "2025-12-02"},
            {"txn_id": "P003", "account_id": "A002", "amount": 5000.0, "txn_type": "credit", "date": "2025-12-03"},
        ]

        df = create_dataframe(spark, txns)
        df = add_signed_amount(df)
        df = with_date_as_timestamp(df, date_col="date")

        print("--- Base DataFrame schema:")
        show_schema(df)

        print("--- Net balance by account:")
        compute_net_balance_by_account(df).show(truncate=False)

        print("--- Monthly balance:")
        compute_monthly_balance(df).show(truncate=False)

        register_temp_view(df, "transactions")
        execute_sql(spark, "SELECT account_id, SUM(signed_amount) AS net FROM transactions GROUP BY account_id").show(truncate=False)

        print("--- Rolling balance:")
        accumulate_rolling_balance(df).show(truncate=False)

        print("--- Pivot sample:")
        pivot_df = pivot_by_category(df, "account_id", "txn_type", "amount")
        pivot_df.show(truncate=False)

        ml_data = [
            {"feature1": 2.0, "feature2": 5.0, "label": 7.0},
            {"feature1": 3.0, "feature2": 6.0, "label": 9.0},
            {"feature1": 4.0, "feature2": 4.0, "label": 8.0},
        ]

        model = ml_pipeline_linear_regression(create_dataframe(spark, ml_data), ["feature1", "feature2"], label_col="label")
        spark_ml_predict(model, create_dataframe(spark, ml_data)).select("features", "label", "prediction").show(truncate=False)

    finally:
        stop_spark_session(spark)
