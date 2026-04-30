"""
Auto-generated PySpark code for mapping: M_BFSI_CUSTOMER_360
Description: 
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

class MBfsiCustomer360:
    """Mapping: M_BFSI_CUSTOMER_360"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = self._setup_logger()

    def execute(self):
        """Execute M_BFSI_CUSTOMER_360 mapping"""
        try:
            self.logger.info("Starting M_BFSI_CUSTOMER_360")

            # Source Qualifier: SQ_CUSTOMER
            # No additional processing needed for source qualifier

            # Expression: EXP_DERIVED_FIELDS
            df_exp_derived_fields = df.selectExpr(
                "when(TXN_AMOUNT > 100000, 'HIGH', 'NORMAL') as TXN_FLAG",
                "when(ISNULL(TXN_AMOUNT), 0, 1) as VALID_TXN"
            )

            # Aggregator: AGG_TXN
            # Simple Aggregation
            df_agg = df.agg(
                F.sum("TXN_AMOUNT").alias("TOTAL_TXN"), F.avg("TXN_AMOUNT").alias("AVG_TXN"), F.max("TXN_AMOUNT").alias("MAX_TXN")
            )
            

            self.logger.info("Mapping completed successfully")
        except Exception as e:
            self.logger.error(f"Mapping failed: {str(e)}", exc_info=True)
            raise

    def _setup_logger(self):
        import logging
        logger = logging.getLogger(self.__class__.__name__)
        return logger