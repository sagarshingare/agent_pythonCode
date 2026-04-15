"""
PySpark Implementation of Informatica Mapping M_BFSI_CUSTOMER_360

This script converts the Informatica ETL mapping to PySpark DataFrame operations.
It processes customer transaction data, applies business logic, and generates aggregated results.

Production Features:
- Comprehensive logging
- Error handling and validation
- Schema enforcement
- Performance optimizations
- Configurable parameters
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, max, when, lit, current_date,
    to_timestamp, isnan, isnull
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType,
    StringType, TimestampType, DateType
)

BASE_DIR = Path(__file__).resolve().parent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(BASE_DIR / 'customer_360_etl.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class Customer360ETL:
    """
    ETL class for processing customer transaction data and generating 360-degree view.
    Implements the logic from Informatica mapping M_BFSI_CUSTOMER_360.
    """

    def __init__(self, spark_session, config=None):
        """
        Initialize ETL processor with Spark session and configuration.

        Args:
            spark_session: Active SparkSession
            config: Dictionary containing configuration parameters
        """
        self.spark = spark_session
        self.config = config or self._get_default_config()

        # Define source schema
        self.source_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), False),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TXN_AMOUNT", DoubleType(), True),
            StructField("TXN_DATE", DateType(), True),
            StructField("LAST_UPDATED_TS", TimestampType(), True)
        ])

        # Define target schema
        self.target_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), False),
            StructField("TOTAL_TXN", DoubleType(), True),
            StructField("AVG_TXN", DoubleType(), True),
            StructField("MAX_TXN", DoubleType(), True),
            StructField("LOAD_DT", DateType(), True)
        ])

        logger.info("Customer360ETL initialized successfully")

    def _get_default_config(self):
        """Get default configuration parameters."""
        return {
            'source_path': str(BASE_DIR / 'sample_src_customer_txn.csv'),
            'target_path': str(BASE_DIR / 'tgt_customer_agg'),
            'last_successful_run': '2023-01-01 00:00:00',
            'high_txn_threshold': 100000.0,
            'partition_by': ['CUSTOMER_ID'],
            'repartition_count': 4
        }

    def validate_config(self):
        """Validate configuration parameters."""
        required_keys = ['source_path', 'target_path', 'last_successful_run']
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration: {key}")

        logger.info("Configuration validation passed")

    def read_source_data(self):
        """
        Read source data from CSV file with incremental logic.

        Returns:
            DataFrame: Filtered source data
        """
        try:
            logger.info(f"Reading source data from: {self.config['source_path']}")

            # Read CSV with schema enforcement
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(self.source_schema) \
                .csv(self.config['source_path'])

            # Apply incremental filter (Source Qualifier logic)
            last_run_ts = to_timestamp(lit(self.config['last_successful_run']))
            df_incremental = df.filter(col("LAST_UPDATED_TS") > last_run_ts)

            logger.info(f"Read {df_incremental.count()} incremental records")
            return df_incremental

        except Exception as e:
            logger.error(f"Error reading source data: {str(e)}")
            raise

    def add_derived_fields(self, df):
        """
        Add derived fields using Expression transformation logic.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: DataFrame with additional derived columns
        """
        try:
            logger.info("Adding derived fields")

            # Expression transformation: TXN_FLAG and VALID_TXN
            df_derived = df.withColumn(
                "TXN_FLAG",
                when(col("TXN_AMOUNT") > self.config['high_txn_threshold'], "HIGH")
                .otherwise("NORMAL")
            ).withColumn(
                "VALID_TXN",
                when(isnull(col("TXN_AMOUNT")) | isnan(col("TXN_AMOUNT")), 0)
                .otherwise(1)
            )

            logger.info("Derived fields added successfully")
            return df_derived

        except Exception as e:
            logger.error(f"Error adding derived fields: {str(e)}")
            raise

    def validate_and_filter_records(self, df):
        """
        Apply Router transformation logic to filter valid records.

        Args:
            df: Input DataFrame with VALID_TXN column

        Returns:
            DataFrame: Filtered valid records
        """
        try:
            logger.info("Applying validation and filtering")

            # Count records before filtering
            total_count = df.count()
            valid_df = df.filter(col("VALID_TXN") == 1)
            valid_count = valid_df.count()
            reject_count = total_count - valid_count

            logger.info(f"Total records: {total_count}, Valid: {valid_count}, Rejected: {reject_count}")

            # Log rejected records for audit
            if reject_count > 0:
                rejected_df = df.filter(col("VALID_TXN") == 0)
                logger.warning(f"Rejected records sample: {rejected_df.limit(5).collect()}")

            return valid_df

        except Exception as e:
            logger.error(f"Error in validation and filtering: {str(e)}")
            raise

    def aggregate_transactions(self, df):
        """
        Apply Aggregator transformation to calculate transaction metrics.

        Args:
            df: Input DataFrame with valid records

        Returns:
            DataFrame: Aggregated results by CUSTOMER_ID
        """
        try:
            logger.info("Performing transaction aggregation")

            # Aggregator transformation
            agg_df = df.groupBy("CUSTOMER_ID").agg(
                sum("TXN_AMOUNT").alias("TOTAL_TXN"),
                avg("TXN_AMOUNT").alias("AVG_TXN"),
                max("TXN_AMOUNT").alias("MAX_TXN")
            )

            # Add load date
            agg_df = agg_df.withColumn("LOAD_DT", current_date())

            logger.info(f"Aggregated data for {agg_df.count()} customers")
            return agg_df

        except Exception as e:
            logger.error(f"Error in aggregation: {str(e)}")
            raise

    def validate_target_schema(self, df):
        """
        Validate that the DataFrame matches the target schema.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If schema validation fails
        """
        try:
            logger.info("Validating target schema")

            # Check column count
            if len(df.columns) != len(self.target_schema.fields):
                raise ValueError(f"Column count mismatch. Expected: {len(self.target_schema.fields)}, Got: {len(df.columns)}")

            # Check column names and types
            for field in self.target_schema.fields:
                if field.name not in df.columns:
                    raise ValueError(f"Missing required column: {field.name}")

                actual_type = df.schema[field.name].dataType
                expected_type = field.dataType
                if str(actual_type) != str(expected_type):
                    logger.warning(f"Type mismatch for {field.name}: Expected {expected_type}, Got {actual_type}")

            logger.info("Target schema validation passed")

        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            raise

    def write_target_data(self, df):
        """
        Write aggregated data to target location with production optimizations.

        Args:
            df: DataFrame to write
        """
        try:
            logger.info(f"Writing target data to: {self.config['target_path']}")

            # Validate schema before writing
            self.validate_target_schema(df)

            # Write with optimizations
            df.repartition(self.config['repartition_count']) \
              .write \
              .mode("overwrite") \
              .partitionBy(self.config['partition_by']) \
              .parquet(self.config['target_path'])

            logger.info("Target data written successfully")

        except Exception as e:
            logger.error(f"Error writing target data: {str(e)}")
            raise

    def run_etl(self):
        """
        Execute the complete ETL pipeline.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info("Starting Customer 360 ETL pipeline")

            # Validate configuration
            self.validate_config()

            # Step 1: Read source data (Source Qualifier)
            source_df = self.read_source_data()

            # Step 2: Add derived fields (Expression)
            derived_df = self.add_derived_fields(source_df)

            # Step 3: Validate and filter (Router)
            valid_df = self.validate_and_filter_records(derived_df)

            # Step 4: Aggregate transactions (Aggregator)
            agg_df = self.aggregate_transactions(valid_df)

            # Step 5: Write target data
            self.write_target_data(agg_df)

            logger.info("ETL pipeline completed successfully")
            return True

        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            return False

def main():
    """Main function to execute the ETL job."""
    # Initialize Spark session with production configurations
    spark = SparkSession.builder \
        .appName("Customer360ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    try:
        # Initialize ETL processor
        etl_config = {
            'source_path': str(BASE_DIR / 'sample_src_customer_txn.csv'),
            'target_path': str(BASE_DIR / 'tgt_customer_agg'),
            'last_successful_run': '2023-01-01 00:00:00',
            'high_txn_threshold': 100000.0,
            'partition_by': ['CUSTOMER_ID'],
            'repartition_count': 4
        }

        etl_processor = Customer360ETL(spark, etl_config)

        # Run ETL pipeline
        success = etl_processor.run_etl()

        if success:
            logger.info("ETL job completed successfully")
            spark.stop()
            sys.exit(0)
        else:
            logger.error("ETL job failed")
            spark.stop()
            sys.exit(1)

    except Exception as e:
        logger.error(f"Main function error: {str(e)}")
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()