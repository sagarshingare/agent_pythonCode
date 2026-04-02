"""PySpark ETL for Data Engineering Architect project."""

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
except ImportError:
    SparkSession = None
    col = None


def get_spark():
    if SparkSession is None:
        raise ImportError("pyspark not installed")
    return SparkSession.builder.appName("data_engineering_architect").master("local[1]").getOrCreate()


def load_and_transform(transactions: list):
    spark = get_spark()
    df = spark.createDataFrame(transactions)
    df = df.withColumn("signed_amount", col("amount") * (col("type") == "credit").cast("integer")*2 - col("amount")*(col("type") == "debit").cast("integer"))
    df.show()
    spark.stop()


def example() -> None:
    print("--- DATA ENG ARCH PYSPARK ETL ---")
    try:
        load_and_transform([
            {"account_id": "A1", "amount": 100, "type": "credit"},
            {"account_id": "A1", "amount": 50, "type": "debit"},
        ])
    except Exception as exc:
        print("pyspark unavailable", exc)
