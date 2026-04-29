"""
Main PySpark Job Orchestrator
Auto-generated from Informatica mappings
"""

from pyspark.sql import SparkSession
import logging

def setup_spark_session() -> SparkSession:
    """Setup Spark session"""
    spark = SparkSession.builder \
        .appName("Informatica_Migration") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    return spark

def main():
    """Main entry point"""
    logger = logging.getLogger(__name__)
    logger.info("Starting Informatica migration job")
    
    spark = setup_spark_session()
    
    # Execute 3 mapping(s)
    # Mapping: m_COMPTIME_Build_Message_Counters
    # MComptimeBuildMessageCounters(spark).execute()

    # Mapping: m_COMPTIME_Load_COMP_TIME_DAILY_TBL
    # MComptimeLoadCompTimeDailyTbl(spark).execute()

    # Mapping: m_COMPTIME_Current_Pay_Period
    # MComptimeCurrentPayPeriod(spark).execute()

    logger.info("Job completed successfully")
    spark.stop()

if __name__ == "__main__":
    main()