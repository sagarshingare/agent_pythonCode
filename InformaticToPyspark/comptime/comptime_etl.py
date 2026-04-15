"""
PySpark Implementation of Informatica CompTime ETL System

This script converts the Informatica CompTime ETL mappings to PySpark DataFrame operations.
It processes compensatory time data from flat files, validates records, loads to database tables,
and generates counters and messages.

Production Features:
- Comprehensive logging with different levels
- Error handling and validation
- Schema enforcement
- Performance optimizations
- Modular architecture
- Configuration management
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_date, to_timestamp, concat, lpad,
    count, current_timestamp, isnan, isnull, length
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType
)

BASE_DIR = Path(__file__).resolve().parent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(BASE_DIR / 'comptime_etl.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CompTimeETL:
    """
    ETL class for processing CompTime (compensatory time) data.

    Implements the logic from three Informatica mappings:
    1. m_COMPTIME_Current_Pay_Period - Gets current pay period
    2. m_COMPTIME_Load_COMP_TIME_DAILY_TBL - Loads comp time data
    3. m_COMPTIME_Build_Message_Counters - Counts records and builds messages
    """

    def __init__(self, spark_session, config=None):
        """
        Initialize CompTime ETL processor.

        Args:
            spark_session: Active SparkSession
            config: Dictionary containing configuration parameters
        """
        self.spark = spark_session
        self.config = config or self._get_default_config()

        # Define source schema for CompTime flat file
        self.comptime_source_schema = StructType([
            StructField("SSN", StringType(), False),
            StructField("NAME", StringType(), True),
            StructField("CURRENT_ACCT", StringType(), True),
            StructField("CURRENT_ORG", StringType(), True),
            StructField("FLSA_STATUS", StringType(), True),
            StructField("COMP_TIME_CUR_BAL", DoubleType(), True),
            StructField("COMP_TIME_YEAR_EARNED", IntegerType(), True),
            StructField("PP_END_DATE", StringType(), True),
            StructField("DAILY_DATE_EARNED", StringType(), True),
            StructField("COMP_TIME_RATE", DoubleType(), True),
            StructField("COMP_TIME_HOURS", DoubleType(), True),
            StructField("COMP_TIME_UNDEF", DoubleType(), True)
        ])

        # Define PAY_PERIOD schema
        self.pay_period_schema = StructType([
            StructField("PP_NUM", IntegerType(), False),
            StructField("PP_END_YEAR", IntegerType(), False),
            StructField("PP_START_DTE", TimestampType(), True),
            StructField("PP_END_DTE", TimestampType(), True),
            StructField("LV_NUM", IntegerType(), True),
            StructField("LV_YEAR", IntegerType(), True),
            StructField("PAY_DTE", TimestampType(), True),
            StructField("CURR_PP_FLAG", StringType(), True),
            StructField("HOLIDAY_1", TimestampType(), True),
            StructField("HOLIDAY_2", TimestampType(), True)
        ])

        # Define target schemas
        self.comp_time_daily_schema = StructType([
            StructField("PP_END_YEAR", IntegerType(), True),
            StructField("PP_NUM", IntegerType(), True),
            StructField("PP_YEAR_NUM", IntegerType(), True),
            StructField("SSN", StringType(), True),
            StructField("NAME", StringType(), True),
            StructField("CURRENT_ACCT", StringType(), True),
            StructField("CURRENT_ORG", StringType(), True),
            StructField("FLSA_STATUS", StringType(), True),
            StructField("COMP_TIME_CUR_BAL", DoubleType(), True),
            StructField("COMP_TIME_YEAR_EARNED", IntegerType(), True),
            StructField("PP_END_DATE", DateType(), True),
            StructField("DAILY_DATE_EARNED", DateType(), True),
            StructField("COMP_TIME_RATE", DoubleType(), True),
            StructField("COMP_TIME_HOURS", DoubleType(), True),
            StructField("COMP_TIME_UNDEF", DoubleType(), True)
        ])

        self.counter_schema = StructType([
            StructField("RUN_DATE", TimestampType(), True),
            StructField("PROCESS_NAME", StringType(), True),
            StructField("COUNTER_DESCRIPTION", StringType(), True),
            StructField("COUNTER_VALUE", IntegerType(), True),
            StructField("PP_END_YEAR", IntegerType(), True),
            StructField("PP_NUM", IntegerType(), True),
            StructField("CYCLE_ID", IntegerType(), True)
        ])

        logger.info("CompTimeETL initialized successfully")

    def _get_default_config(self):
        """Get default configuration parameters."""
        return {
            'comptime_source_path': str(BASE_DIR / 'sample_comptime_data.csv'),
            'pay_period_path': str(BASE_DIR / 'sample_pay_period.csv'),
            'comp_time_daily_target': str(BASE_DIR / 'comp_time_daily_tbl'),
            'counter_target': str(BASE_DIR / 'counter_tbl'),
            'message_file_target': str(BASE_DIR / 'comptime_message_file.txt'),
            'date_file_target': str(BASE_DIR / 'comp_time_date_file.txt'),
            'repartition_count': 4,
            'batch_size': 10000
        }

    def validate_config(self):
        """Validate configuration parameters."""
        required_keys = [
            'comptime_source_path', 'pay_period_path',
            'comp_time_daily_target', 'counter_target'
        ]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration: {key}")

        logger.info("Configuration validation passed")

    def get_current_pay_period(self):
        """
        Get current pay period information (equivalent to m_COMPTIME_Current_Pay_Period).

        Returns:
            dict: Current pay period information
        """
        try:
            logger.info("Getting current pay period information")

            # Read PAY_PERIOD data
            pay_period_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(self.pay_period_schema) \
                .csv(self.config['pay_period_path'])

            # Filter for current pay period (CURR_PP_FLAG = 'Y')
            current_pp_df = pay_period_df.filter(col("CURR_PP_FLAG") == "Y")

            if current_pp_df.count() == 0:
                raise ValueError("No current pay period found")

            current_pp = current_pp_df.first()

            # Build pay period string (PP_END_YEAR + padded PP_NUM)
            pp_year_num = f"{current_pp.PP_END_YEAR}{current_pp.PP_NUM:02d}"

            pay_period_info = {
                'PP_NUM': current_pp.PP_NUM,
                'PP_END_YEAR': current_pp.PP_END_YEAR,
                'PP_YEAR_NUM': pp_year_num,
                'PP_START_DTE': current_pp.PP_START_DTE,
                'PP_END_DTE': current_pp.PP_END_DTE
            }

            logger.info(f"Current pay period: {pp_year_num}")
            return pay_period_info

        except Exception as e:
            logger.error(f"Error getting current pay period: {str(e)}")
            raise

    def validate_comptime_data(self, df):
        """
        Validate CompTime data records (equivalent to exp_Initial transformation).

        Args:
            df: Input CompTime DataFrame

        Returns:
            DataFrame: DataFrame with validation flags
        """
        try:
            logger.info("Validating CompTime data")

            # Add validation flag (equivalent to DECODE(IS_NUMBER(SSN), 'D', 'NO'))
            validated_df = df.withColumn(
                "VALID_RECORD_FLAG",
                when(length(col("SSN")) == 9, 1)  # Simple SSN validation
                .otherwise(0)
            ).withColumn(
                "CURR_PP_FLAG",
                lit("Y")  # Set current pay period flag
            )

            # Count valid/invalid records
            valid_count = validated_df.filter(col("VALID_RECORD_FLAG") == 1).count()
            invalid_count = validated_df.filter(col("VALID_RECORD_FLAG") == 0).count()

            logger.info(f"Validation complete - Valid: {valid_count}, Invalid: {invalid_count}")
            return validated_df

        except Exception as e:
            logger.error(f"Error validating CompTime data: {str(e)}")
            raise

    def filter_valid_records(self, df):
        """
        Filter only valid records (equivalent to fil_Valid_Records).

        Args:
            df: Input DataFrame with VALID_RECORD_FLAG

        Returns:
            DataFrame: Filtered valid records
        """
        try:
            logger.info("Filtering valid records")

            valid_df = df.filter(col("VALID_RECORD_FLAG") == 1)
            logger.info(f"Filtered {valid_df.count()} valid records")
            return valid_df

        except Exception as e:
            logger.error(f"Error filtering valid records: {str(e)}")
            raise

    def convert_data_types(self, df, pay_period_info):
        """
        Convert data types and add derived fields (equivalent to exp_Convert).

        Args:
            df: Input DataFrame
            pay_period_info: Current pay period information

        Returns:
            DataFrame: DataFrame with converted data types
        """
        try:
            logger.info("Converting data types and adding derived fields")

            # Convert date strings to date types
            converted_df = df.withColumn(
                "PP_END_DATE",
                to_date(col("PP_END_DATE"), "yyyyMMdd")
            ).withColumn(
                "DAILY_DATE_EARNED",
                to_date(col("DAILY_DATE_EARNED"), "yyyyMMdd")
            )

            # Add pay period derived fields
            converted_df = converted_df.withColumn(
                "PP_END_YEAR",
                lit(pay_period_info['PP_END_YEAR'])
            ).withColumn(
                "PP_NUM",
                lit(pay_period_info['PP_NUM'])
            ).withColumn(
                "PP_YEAR_NUM",
                lit(int(pay_period_info['PP_YEAR_NUM']))
            )

            logger.info("Data type conversion completed")
            return converted_df

        except Exception as e:
            logger.error(f"Error converting data types: {str(e)}")
            raise

    def load_comp_time_daily_table(self, df):
        """
        Load data to COMP_TIME_DAILY_TBL (equivalent to m_COMPTIME_Load_COMP_TIME_DAILY_TBL).

        Args:
            df: Input DataFrame to load

        Returns:
            int: Number of records loaded
        """
        try:
            logger.info(f"Loading data to COMP_TIME_DAILY_TBL: {self.config['comp_time_daily_target']}")

            # Select only columns that are in the target schema
            target_columns = [field.name for field in self.comp_time_daily_schema.fields]
            df_selected = df.select(*target_columns)

            # Validate schema before loading
            self.validate_target_schema(df_selected, self.comp_time_daily_schema)

            # Write to target
            record_count = df_selected.count()
            df_selected.repartition(self.config['repartition_count']) \
              .write \
              .mode("overwrite") \
              .parquet(self.config['comp_time_daily_target'])

            logger.info(f"Loaded {record_count} records to COMP_TIME_DAILY_TBL")
            return record_count

        except Exception as e:
            logger.error(f"Error loading COMP_TIME_DAILY_TBL: {str(e)}")
            raise

    def build_message_counters(self, record_count, pay_period_info):
        """
        Build message and counter data (equivalent to m_COMPTIME_Build_Message_Counters).

        Args:
            record_count: Number of records processed
            pay_period_info: Current pay period information

        Returns:
            tuple: (counter_df, subject, message)
        """
        try:
            logger.info("Building message and counters")

            # Import datetime for current timestamp
            from datetime import datetime

            # Create counter data (equivalent to exp_Counters and exp_Final)
            counter_data = [{
                'RUN_DATE': datetime.now(),
                'PROCESS_NAME': 'm_COMPTIME_Build_Message_Counters',
                'COUNTER_DESCRIPTION': 'Number of detail records from the COMP TIME file.',
                'COUNTER_VALUE': record_count,
                'PP_END_YEAR': pay_period_info['PP_END_YEAR'],
                'PP_NUM': pay_period_info['PP_NUM'],
                'CYCLE_ID': 1
            }]

            counter_df = self.spark.createDataFrame(counter_data, self.counter_schema)

            # Build subject and message (equivalent to exp_Build_Message)
            environment = "Dev:"  # In real implementation, this would be dynamic
            subject = f"{environment} Comp Time File loaded successfully for Pay Period: {pay_period_info['PP_YEAR_NUM']}"
            message = f"Number of Detail Records from Comp Time file\t= {record_count}"

            logger.info(f"Built message - Subject: {subject}")
            return counter_df, subject, message

        except Exception as e:
            logger.error(f"Error building message counters: {str(e)}")
            raise

    def load_counter_table(self, counter_df):
        """
        Load counter data to COUNTER_TBL.

        Args:
            counter_df: Counter DataFrame to load
        """
        try:
            logger.info(f"Loading counter data to COUNTER_TBL: {self.config['counter_target']}")

            # Validate schema
            self.validate_target_schema(counter_df, self.counter_schema)

            # Write to target
            counter_df.repartition(self.config['repartition_count']) \
              .write \
              .mode("overwrite") \
              .parquet(self.config['counter_target'])

            logger.info("Counter data loaded successfully")

        except Exception as e:
            logger.error(f"Error loading counter table: {str(e)}")
            raise

    def write_message_file(self, subject, message):
        """
        Write message to flat file (equivalent to COMPTIME_MESSAGE_FILE).

        Args:
            subject: Email subject
            message: Email message
        """
        try:
            logger.info(f"Writing message file: {self.config['message_file_target']}")

            message_content = f"SUBJECT: {subject}\nMESSAGE: {message}\n"

            # Write to file
            with open(self.config['message_file_target'], 'w') as f:
                f.write(message_content)

            logger.info("Message file written successfully")

        except Exception as e:
            logger.error(f"Error writing message file: {str(e)}")
            raise

    def write_date_file(self, pay_period_info):
        """
        Write pay period date file (equivalent to COMP_TIME_DATE_FILE).

        Args:
            pay_period_info: Current pay period information
        """
        try:
            logger.info(f"Writing date file: {self.config['date_file_target']}")

            date_content = pay_period_info['PP_YEAR_NUM']

            # Write to file
            with open(self.config['date_file_target'], 'w') as f:
                f.write(date_content)

            logger.info("Date file written successfully")

        except Exception as e:
            logger.error(f"Error writing date file: {str(e)}")
            raise

    def validate_target_schema(self, df, expected_schema):
        """
        Validate DataFrame schema against expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema

        Raises:
            ValueError: If schema validation fails
        """
        try:
            logger.info("Validating target schema")

            # Check column count
            if len(df.columns) != len(expected_schema.fields):
                raise ValueError(f"Column count mismatch. Expected: {len(expected_schema.fields)}, Got: {len(df.columns)}")

            # Check column names and types
            expected_columns = [field.name for field in expected_schema.fields]
            actual_columns = df.columns

            if set(expected_columns) != set(actual_columns):
                missing = set(expected_columns) - set(actual_columns)
                extra = set(actual_columns) - set(expected_columns)
                raise ValueError(f"Column mismatch. Missing: {missing}, Extra: {extra}")

            logger.info("Target schema validation passed")

        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            raise

    def run_etl(self):
        """
        Execute the complete CompTime ETL workflow.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info("Starting CompTime ETL workflow")

            # Validate configuration
            self.validate_config()

            # Step 1: Get current pay period (m_COMPTIME_Current_Pay_Period)
            pay_period_info = self.get_current_pay_period()

            # Step 2: Read and validate CompTime source data
            logger.info(f"Reading CompTime source data from: {self.config['comptime_source_path']}")
            comptime_df = self.spark.read \
                .option("header", "false") \
                .option("inferSchema", "false") \
                .schema(self.comptime_source_schema) \
                .csv(self.config['comptime_source_path'])

            # Step 3: Validate data (exp_Initial)
            validated_df = self.validate_comptime_data(comptime_df)

            # Step 4: Filter valid records (fil_Valid_Records)
            valid_df = self.filter_valid_records(validated_df)

            # Step 5: Convert data types (exp_Convert)
            converted_df = self.convert_data_types(valid_df, pay_period_info)

            # Step 6: Load to COMP_TIME_DAILY_TBL (m_COMPTIME_Load_COMP_TIME_DAILY_TBL)
            record_count = self.load_comp_time_daily_table(converted_df)

            # Step 7: Build message and counters (m_COMPTIME_Build_Message_Counters)
            counter_df, subject, message = self.build_message_counters(record_count, pay_period_info)

            # Step 8: Load counter data
            self.load_counter_table(counter_df)

            # Step 9: Write message file
            self.write_message_file(subject, message)

            # Step 10: Write date file (from current pay period mapping)
            self.write_date_file(pay_period_info)

            logger.info("CompTime ETL workflow completed successfully")
            return True

        except Exception as e:
            logger.error(f"ETL workflow failed: {str(e)}")
            return False

def main():
    """Main function to execute the CompTime ETL job."""
    # Initialize Spark session with production configurations
    spark = SparkSession.builder \
        .appName("CompTimeETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    try:
        # Initialize ETL processor
        etl_config = {
            'comptime_source_path': str(BASE_DIR / 'sample_comptime_data.csv'),
            'pay_period_path': str(BASE_DIR / 'sample_pay_period.csv'),
            'comp_time_daily_target': str(BASE_DIR / 'comp_time_daily_tbl'),
            'counter_target': str(BASE_DIR / 'counter_tbl'),
            'message_file_target': str(BASE_DIR / 'comptime_message_file.txt'),
            'date_file_target': str(BASE_DIR / 'comp_time_date_file.txt'),
            'repartition_count': 4,
            'batch_size': 10000
        }

        etl_processor = CompTimeETL(spark, etl_config)

        # Run ETL workflow
        success = etl_processor.run_etl()

        if success:
            logger.info("CompTime ETL job completed successfully")
            spark.stop()
            sys.exit(0)
        else:
            logger.error("CompTime ETL job failed")
            spark.stop()
            sys.exit(1)

    except Exception as e:
        logger.error(f"Main function error: {str(e)}")
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()