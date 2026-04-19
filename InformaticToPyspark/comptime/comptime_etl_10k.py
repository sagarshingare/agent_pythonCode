"""
INFORMATICA CompTime ETL SUITE - PySpark Migration
======================================================================

MIGRATION NOTES:
- Mappings: m_COMPTIME_Current_Pay_Period, m_COMPTIME_Load_COMP_TIME_DAILY_TBL, 
            m_COMPTIME_Build_Message_Counters
- Source: Informatica COMP_TIME folder
- Target: Production-grade Apache Spark DataFrame operations
- Business Logic: 100% preserved with identical transformation sequence

INFORMATICA MAPPING COMPONENTS:

MAPPING 1: m_COMPTIME_Current_Pay_Period
  Purpose: Dynamically determine current pay period
  Key Logic: SELECT * FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'
  Output: Current pay period parameters

MAPPING 2: m_COMPTIME_Load_COMP_TIME_DAILY_TBL
  Transformations:
  1. SQ_U0287D01 (Source Qualifier) -> Read flat file
  2. exp_Initial (Expression) -> Validate SSN
  3. fil_Valid_Records (Filter) -> Router logic
  4. exp_Convert (Expression) -> Type conversion
  5. Loads to COMP_TIME_DAILY_TBL

MAPPING 3: m_COMPTIME_Build_Message_Counters
  Transformations:
  1. agg_ALL_RECORDS (Aggregator) -> COUNT records
  2. exp_Counters (Expression) -> Build counter descriptions
  3. Loads to COUNTER_TBL and message file

PRODUCTION FEATURES:
- Comprehensive structured logging
- Error handling and data quality checks
- Dynamic pay period determination
- Counter and message generation
- Performance optimization
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, sum as spark_sum, count, current_timestamp,
    to_date, isnull, length
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, DateType, 
    TimestampType, StringType
)

BASE_DIR = Path(__file__).resolve().parent

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

def setup_logging():
    """Configure comprehensive logging with file and console handlers."""
    log_file = BASE_DIR / 'comptime_etl_10k.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(str(log_file)),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

logger = setup_logging()


# ============================================================================
# INFORMATICA COMPTIME MAPPINGS SUITE - PySpark Implementation
# ============================================================================

class CompTimeETL:
    """
    PySpark Implementation of Informatica CompTime ETL Mappings Suite.
    
    Implements three coordinated Informatica mappings:
    1. m_COMPTIME_Current_Pay_Period - Determine current pay period
    2. m_COMPTIME_Load_COMP_TIME_DAILY_TBL - Load comp time data
    3. m_COMPTIME_Build_Message_Counters - Generate counters and messages
    """

    def __init__(self, spark_session: SparkSession, config: Dict = None):
        """Initialize CompTime ETL processor."""
        self.spark = spark_session
        self.config = config or self._get_default_config()
        
        # Data quality metrics
        self.metrics = {
            'total_records_read': 0,
            'valid_records': 0,
            'rejected_records': 0,
            'records_loaded': 0
        }
        
        # Informatica source schema: U0287D01 (Flat File)
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
        
        # Informatica source schema: PAY_PERIOD
        self.pay_period_schema = StructType([
            StructField("PP_NUM", IntegerType(), False),
            StructField("PP_END_YEAR", IntegerType(), False),
            StructField("PP_START_DTE", TimestampType(), True),
            StructField("PP_END_DTE", TimestampType(), True),
            StructField("LV_NUM", IntegerType(), True),
            StructField("LV_YEAR", IntegerType(), True),
            StructField("PAY_DTE", TimestampType(), True),
            StructField("CURR_PP_FLAG", StringType(), True),
            StructField("HOLIDAY_1", StringType(), True),
            StructField("HOLIDAY_2", StringType(), True)
        ])
        
        # Informatica target schema: COMP_TIME_DAILY_TBL
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
        
        # Informatica target schema: COUNTER_TBL
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

    def _get_default_config(self) -> Dict:
        """Get default configuration parameters."""
        return {
            'comptime_source_path': str(BASE_DIR / 'sample_comptime_data_10k.csv'),
            'pay_period_path': str(BASE_DIR / 'sample_pay_period_10k.csv'),
            'comp_time_daily_target': str(BASE_DIR / 'comp_time_daily_tbl_10k'),
            'counter_target': str(BASE_DIR / 'counter_tbl_10k'),
            'message_file_target': str(BASE_DIR / 'comptime_message_file_10k.txt'),
            'date_file_target': str(BASE_DIR / 'comp_time_date_file_10k.txt'),
            'reject_path': str(BASE_DIR / 'comptime_rejects_10k'),
            'repartition_count': 8,
            'batch_size': 50000
        }

    # ========================================================================
    # MAPPING 1: m_COMPTIME_Current_Pay_Period
    # ========================================================================

    def get_current_pay_period(self) -> Dict:
        """
        INFORMATICA EQUIVALENT: m_COMPTIME_Current_Pay_Period
        Purpose: Determine current pay period from reference table
        SQL: SELECT * FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'
        """
        try:
            logger.info("MAPPING 1: Getting current pay period (m_COMPTIME_Current_Pay_Period)")
            
            # Read PAY_PERIOD reference table
            pay_period_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(self.pay_period_schema) \
                .csv(self.config['pay_period_path'])
            
            # Filter for current pay period
            current_pp_df = pay_period_df.filter(col("CURR_PP_FLAG") == "Y")
            
            if current_pp_df.count() == 0:
                raise ValueError("No current pay period found - check CURR_PP_FLAG")
            
            current_pp = current_pp_df.first()
            
            # Build pay period identifier (YYYYMM format)
            pp_year_num = f"{current_pp.PP_END_YEAR}{current_pp.PP_NUM:02d}"
            
            pay_period_info = {
                'PP_NUM': current_pp.PP_NUM,
                'PP_END_YEAR': current_pp.PP_END_YEAR,
                'PP_YEAR_NUM': pp_year_num,
                'PP_START_DTE': current_pp.PP_START_DTE,
                'PP_END_DTE': current_pp.PP_END_DTE
            }
            
            logger.info(f"Current pay period determined: {pp_year_num}")
            return pay_period_info
        
        except Exception as e:
            logger.error(f"Error in current pay period mapping: {str(e)}", exc_info=True)
            raise

    # ========================================================================
    # MAPPING 2: m_COMPTIME_Load_COMP_TIME_DAILY_TBL
    # ========================================================================

    def read_comptime_source(self) -> str:
        """
        INFORMATICA EQUIVALENT: SQ_U0287D01 (Source Qualifier)
        Read CompTime source flat file
        """
        try:
            logger.info("MAPPING 2: Reading CompTime source (SQ_U0287D01)")
            
            source_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(self.comptime_source_schema) \
                .csv(self.config['comptime_source_path'])
            
            record_count = source_df.count()
            self.metrics['total_records_read'] = record_count
            
            logger.info(f"Read {record_count} CompTime source records")
            return source_df
        
        except Exception as e:
            logger.error(f"Error reading CompTime source: {str(e)}", exc_info=True)
            raise

    def validate_comptime_data(self, df: str) -> str:
        """
        INFORMATICA EQUIVALENT: exp_Initial (Expression Transformation)
        Validate CompTime records - SSN format validation
        Informatica Logic: Validate that SSN is 9 digits
        """
        try:
            logger.info("Validating CompTime data (exp_Initial)")
            
            # Add validation flag: 1 if SSN length is 9, else 0
            validated_df = df.withColumn(
                "VALID_RECORD_FLAG",
                when(length(col("SSN")) == 9, 1)
                   .otherwise(0)
            )
            
            valid_count = validated_df.filter(col("VALID_RECORD_FLAG") == 1).count()
            invalid_count = validated_df.filter(col("VALID_RECORD_FLAG") == 0).count()
            
            logger.info(f"Validation complete - Valid: {valid_count}, Invalid: {invalid_count}")
            return validated_df
        
        except Exception as e:
            logger.error(f"Error validating CompTime data: {str(e)}", exc_info=True)
            raise

    def filter_valid_records(self, df: str) -> Tuple:
        """
        INFORMATICA EQUIVALENT: fil_Valid_Records (Filter) + Router logic
        Split into valid and invalid records
        """
        try:
            logger.info("Filtering valid records (fil_Valid_Records)")
            
            valid_df = df.filter(col("VALID_RECORD_FLAG") == 1)
            reject_df = df.filter(col("VALID_RECORD_FLAG") == 0)
            
            valid_count = valid_df.count()
            reject_count = reject_df.count()
            
            self.metrics['valid_records'] = valid_count
            self.metrics['rejected_records'] = reject_count
            
            logger.info(f"Filtered {valid_count} valid records, {reject_count} rejected")
            return valid_df, reject_df
        
        except Exception as e:
            logger.error(f"Error filtering records: {str(e)}", exc_info=True)
            raise

    def convert_data_types(self, df: str, pay_period_info: Dict) -> str:
        """
        INFORMATICA EQUIVALENT: exp_Convert (Expression Transformation)
        Convert data types and add pay period derived fields
        - Convert PP_END_DATE from 'yyyyMMdd' to Date
        - Convert DAILY_DATE_EARNED from 'yyyyMMdd' to Date
        - Add PP_END_YEAR, PP_NUM, PP_YEAR_NUM
        """
        try:
            logger.info("Converting data types (exp_Convert)")
            
            # Convert date strings to Date type
            converted_df = df.withColumn(
                "PP_END_DATE",
                to_date(col("PP_END_DATE"), "yyyyMMdd")
            ).withColumn(
                "DAILY_DATE_EARNED",
                to_date(col("DAILY_DATE_EARNED"), "yyyyMMdd")
            )
            
            # Add pay period fields
            converted_df = converted_df \
                .withColumn("PP_END_YEAR", lit(pay_period_info['PP_END_YEAR'])) \
                .withColumn("PP_NUM", lit(pay_period_info['PP_NUM'])) \
                .withColumn("PP_YEAR_NUM", lit(int(pay_period_info['PP_YEAR_NUM'])))
            
            logger.info("Data type conversion completed")
            return converted_df
        
        except Exception as e:
            logger.error(f"Error converting data types: {str(e)}", exc_info=True)
            raise

    def load_comp_time_daily_table(self, df: str) -> int:
        """
        INFORMATICA EQUIVALENT: m_COMPTIME_Load_COMP_TIME_DAILY_TBL (Target)
        Load processed CompTime data to COMP_TIME_DAILY_TBL
        """
        try:
            logger.info(f"Loading data to COMP_TIME_DAILY_TBL: {self.config['comp_time_daily_target']}")
            
            # Select only target schema columns
            target_columns = [field.name for field in self.comp_time_daily_schema.fields]
            df_selected = df.select(*target_columns)
            
            record_count = df_selected.count()
            self.metrics['records_loaded'] = record_count
            
            # Write to target
            df_selected.repartition(self.config['repartition_count']) \
                .write \
                .mode("overwrite") \
                .parquet(self.config['comp_time_daily_target'])
            
            logger.info(f"Loaded {record_count} records to COMP_TIME_DAILY_TBL")
            return record_count
        
        except Exception as e:
            logger.error(f"Error loading COMP_TIME_DAILY_TBL: {str(e)}", exc_info=True)
            raise

    def write_reject_data(self, reject_df: str) -> bool:
        """Write rejected records for audit trail."""
        try:
            if reject_df.count() == 0:
                logger.info("No rejected records")
                return True
            
            logger.info(f"Writing {reject_df.count()} rejected records")
            
            reject_df.repartition(1) \
                .write \
                .mode("overwrite") \
                .csv(self.config['reject_path'], header=True)
            
            logger.info("Rejected records written successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error writing reject data: {str(e)}", exc_info=True)
            return False

    # ========================================================================
    # MAPPING 3: m_COMPTIME_Build_Message_Counters
    # ========================================================================

    def build_message_counters(self, record_count: int, pay_period_info: Dict) -> Tuple:
        """
        INFORMATICA EQUIVALENT: m_COMPTIME_Build_Message_Counters
        Build audit counters and email message
        
        Transformations:
        1. agg_ALL_RECORDS (Aggregator) -> COUNT(*)
        2. exp_Counters (Expression) -> Build counter descriptions
        3. exp_Build_Message (Expression) -> Build email subject/message
        """
        try:
            logger.info("MAPPING 3: Building message and counters (m_COMPTIME_Build_Message_Counters)")
            
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
            
            # Build email message (equivalent to exp_Build_Message)
            environment = "Dev:"
            subject = f"{environment} Comp Time File loaded successfully for Pay Period: {pay_period_info['PP_YEAR_NUM']}"
            message = f"Number of Detail Records from Comp Time file\t= {record_count}"
            
            logger.info(f"Counter and message built - Count: {record_count}")
            return counter_df, subject, message
        
        except Exception as e:
            logger.error(f"Error building message counters: {str(e)}", exc_info=True)
            raise

    def load_counter_table(self, counter_df: str) -> bool:
        """Load counter data to COUNTER_TBL."""
        try:
            logger.info(f"Loading counter data to COUNTER_TBL: {self.config['counter_target']}")
            
            counter_df.repartition(self.config['repartition_count']) \
                .write \
                .mode("overwrite") \
                .parquet(self.config['counter_target'])
            
            logger.info("Counter data loaded successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error loading counter table: {str(e)}", exc_info=True)
            return False

    def write_message_file(self, subject: str, message: str) -> bool:
        """Write message file (equivalent to COMPTIME_MESSAGE_FILE target)."""
        try:
            logger.info(f"Writing message file: {self.config['message_file_target']}")
            
            message_content = f"SUBJECT: {subject}\nMESSAGE: {message}\n"
            
            with open(self.config['message_file_target'], 'w') as f:
                f.write(message_content)
            
            logger.info("Message file written successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error writing message file: {str(e)}", exc_info=True)
            return False

    def write_date_file(self, pay_period_info: Dict) -> bool:
        """Write date file (equivalent to COMP_TIME_DATE_FILE target)."""
        try:
            logger.info(f"Writing date file: {self.config['date_file_target']}")
            
            date_content = pay_period_info['PP_YEAR_NUM']
            
            with open(self.config['date_file_target'], 'w') as f:
                f.write(date_content)
            
            logger.info("Date file written successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error writing date file: {str(e)}", exc_info=True)
            return False

    def print_metrics(self):
        """Print ETL execution metrics."""
        logger.info("=" * 80)
        logger.info("ETL EXECUTION METRICS (10K RECORDS)")
        logger.info("=" * 80)
        logger.info(f"Total Records Read:        {self.metrics['total_records_read']:,}")
        logger.info(f"Valid Records:             {self.metrics['valid_records']:,}")
        logger.info(f"Rejected Records:          {self.metrics['rejected_records']:,}")
        logger.info(f"Records Loaded:            {self.metrics['records_loaded']:,}")
        logger.info(f"Rejection Rate:            {(self.metrics['rejected_records'] / max(self.metrics['total_records_read'], 1) * 100):.2f}%")
        logger.info("=" * 80)

    def run_etl(self) -> bool:
        """Execute complete CompTime ETL workflow."""
        try:
            logger.info("=" * 80)
            logger.info("STARTING COMPTIME ETL WORKFLOW (10K RECORDS)")
            logger.info("=" * 80)
            
            # Mapping 1: Get current pay period
            pay_period_info = self.get_current_pay_period()
            
            # Mapping 2: Load CompTime data
            logger.info("")
            source_df = self.read_comptime_source()
            validated_df = self.validate_comptime_data(source_df)
            valid_df, reject_df = self.filter_valid_records(validated_df)
            converted_df = self.convert_data_types(valid_df, pay_period_info)
            record_count = self.load_comp_time_daily_table(converted_df)
            self.write_reject_data(reject_df)
            
            # Mapping 3: Build counters and messages
            logger.info("")
            counter_df, subject, message = self.build_message_counters(record_count, pay_period_info)
            self.load_counter_table(counter_df)
            self.write_message_file(subject, message)
            self.write_date_file(pay_period_info)
            
            # Print metrics
            logger.info("")
            self.print_metrics()
            
            logger.info("CompTime ETL workflow completed SUCCESSFULLY")
            return True
        
        except Exception as e:
            logger.error(f"ETL workflow failed: {str(e)}", exc_info=True)
            return False


def main():
    """Main entry point for CompTime ETL."""
    spark = SparkSession.builder \
        .appName("CompTimeETL_10K") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    try:
        logger.info(f"Spark Session initialized - Version: {spark.version}")
        
        etl = CompTimeETL(spark)
        success = etl.run_etl()
        
        spark.stop()
        sys.exit(0 if success else 1)
    
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}", exc_info=True)
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
