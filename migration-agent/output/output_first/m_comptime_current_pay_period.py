"""
Auto-generated PySpark code for mapping: m_COMPTIME_Current_Pay_Period
Description: This mapping returns the Current Pay Period from the Pay Period table.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

class MComptimeCurrentPayPeriod:
    """Mapping: m_COMPTIME_Current_Pay_Period"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = self._setup_logger()

    def execute(self):
        """Execute m_COMPTIME_Current_Pay_Period mapping"""
        try:
            self.logger.info("Starting m_COMPTIME_Current_Pay_Period")

            # Load source: PAY_PERIOD
            df_pay_period = self.spark.read.parquet("/data/sources/pay_period")
            self.logger.info("Loaded source PAY_PERIOD: {} records".format(df_pay_period.count()))

            # Source Qualifier: SQ_PAY_PERIOD
            # No additional processing needed for source qualifier

            # Expression: exp_Build_Pay_Period
            df_exp_build_pay_period = df.selectExpr(
                "when(PP_NUM < 10,
       LPAD(cast_str(PP_NUM), 2, '0'),
    cast_str(PP_NUM)
) as v_PP_NUM",
                "cast_str(PP_END_YEAR) || v_PP_NUM as o_PAY_PERIOD",
                "cast_str(PP_END_YEAR) || v_PP_NUM as v_PAY_PERIOD",
                "SETVARIABLE($$MAP_PP_YEAR_NUM, v_PAY_PERIOD) as o_MAP_PP_YEAR_NUM",
                "SETVARIABLE($$MAP_PP_END_YEAR, PP_END_YEAR) as o_PP_END_YEAR",
                "SETVARIABLE($$MAP_PP_NUM, PP_NUM) as o_PP_NUM"
            )

            # Expression: exp_Final
            df_exp_final = df.selectExpr(
                "PAY_PERIOD as PAY_PERIOD"
            )

            # Write to target: COMP_TIME_DATE_FILE
            df_target.write.mode("overwrite").parquet("/data/targets/comp_time_date_file")
            self.logger.info("Written to target COMP_TIME_DATE_FILE")

            self.logger.info("Mapping completed successfully")
        except Exception as e:
            self.logger.error(f"Mapping failed: {str(e)}", exc_info=True)
            raise

    def _setup_logger(self):
        import logging
        logger = logging.getLogger(self.__class__.__name__)
        return logger