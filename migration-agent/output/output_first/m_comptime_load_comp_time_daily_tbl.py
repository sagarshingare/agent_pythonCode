"""
Auto-generated PySpark code for mapping: m_COMPTIME_Load_COMP_TIME_DAILY_TBL
Description: 
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

class MComptimeLoadCompTimeDailyTbl:
    """Mapping: m_COMPTIME_Load_COMP_TIME_DAILY_TBL"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = self._setup_logger()

    def execute(self):
        """Execute m_COMPTIME_Load_COMP_TIME_DAILY_TBL mapping"""
        try:
            self.logger.info("Starting m_COMPTIME_Load_COMP_TIME_DAILY_TBL")

            # Load source: U0287D01
            df_u0287d01 = self.spark.read.parquet("/data/sources/u0287d01")
            self.logger.info("Loaded source U0287D01: {} records".format(df_u0287d01.count()))

            # Source Qualifier: SQ_U0287D01
            # No additional processing needed for source qualifier

            # Expression: exp_Initial
            df_exp_initial = df.selectExpr(
                "SSN as SSN",
                "NAME as NAME",
                "CURRENT_ACCT as CURRENT_ACCT",
                "CURRENT_ORG as CURRENT_ORG",
                "FLSA_STATUS as FLSA_STATUS",
                "COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL",
                "COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED",
                "PP_END_DATE as PP_END_DATE",
                "DAILY_DATE_EARNED as DAILY_DATE_EARNED",
                "COMP_TIME_RATE as COMP_TIME_RATE",
                "COMP_TIME_HOURS as COMP_TIME_HOURS",
                "COMP_TIME_UNDEF as COMP_TIME_UNDEF",
                "'Y' as o_CURR_PP_FLAG",
                "case_when (TRUE, is_number(SSN), 1,
                                    0)
                        as o_VALID_RECORD_FLAG"
            )

            # Filter: fil_Valid_Records
            df_fil_valid_records = df.filter("VALID_RECORD_FLAG = TRUE")

            # Lookup: lkp_PAY_PERIOD
            # Lookup table: PAY_PERIOD
            # Condition: CURR_PP_FLAG = in_CURR_PP_FLAG
            df_lkp_pay_period = df  # Lookup logic here

            # Expression: exp_Convert
            df_exp_convert = df.selectExpr(
                "SSN as SSN",
                "NAME as NAME",
                "CURRENT_ACCT as CURRENT_ACCT",
                "CURRENT_ORG as CURRENT_ORG",
                "FLSA_STATUS as FLSA_STATUS",
                "COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL",
                "COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED",
                "when(IS_DATE(PP_END_DATE, 'YYYYMMDD'),
                                   to_date(PP_END_DATE, 'YYYYMMDD')
       ) as o_PP_END_DATE",
                "when(IS_DATE(DAILY_DATE_EARNED, 'YYYYMMDD'),
                                   to_date(DAILY_DATE_EARNED, 'YYYYMMDD')
       ) as o_DAILY_DATE_EARNED",
                "COMP_TIME_RATE as COMP_TIME_RATE",
                "COMP_TIME_HOURS as COMP_TIME_HOURS",
                "COMP_TIME_UNDEF as COMP_TIME_UNDEF",
                "lkp_PP_END_YEAR
 as o_PP_END_YEAR",
                "lkp_PP_NUM
 as o_PP_NUM",
                "TO_DECIMAL(
cast_str(lkp_PP_END_YEAR) || 
LPAD(cast_str(lkp_PP_NUM), 2, '0'))
 as o_PP_YEAR_NUM"
            )

            # Expression: exp_Final
            df_exp_final = df.selectExpr(
                "PP_END_YEAR as PP_END_YEAR",
                "PP_NUM as PP_NUM",
                "PP_YEAR_NUM as PP_YEAR_NUM",
                "SSN as SSN",
                "NAME as NAME",
                "CURRENT_ACCT as CURRENT_ACCT",
                "CURRENT_ORG as CURRENT_ORG",
                "FLSA_STATUS as FLSA_STATUS",
                "COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL",
                "COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED",
                "PP_END_DATE as PP_END_DATE",
                "DAILY_DATE_EARNED as DAILY_DATE_EARNED",
                "COMP_TIME_RATE as COMP_TIME_RATE",
                "COMP_TIME_HOURS as COMP_TIME_HOURS",
                "COMP_TIME_UNDEF as COMP_TIME_UNDEF"
            )

            # Write to target: COMP_TIME_DAILY_TBL
            df_target.write.mode("overwrite").parquet("/data/targets/comp_time_daily_tbl")
            self.logger.info("Written to target COMP_TIME_DAILY_TBL")

            self.logger.info("Mapping completed successfully")
        except Exception as e:
            self.logger.error(f"Mapping failed: {str(e)}", exc_info=True)
            raise

    def _setup_logger(self):
        import logging
        logger = logging.getLogger(self.__class__.__name__)
        return logger