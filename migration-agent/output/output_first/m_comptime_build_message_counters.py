"""
Auto-generated PySpark code for mapping: m_COMPTIME_Build_Message_Counters
Description: This mapping gets the count of detail records on the CompTime file that was processed and loads it to the Counters Table. 
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

class MComptimeBuildMessageCounters:
    """Mapping: m_COMPTIME_Build_Message_Counters"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = self._setup_logger()

    def execute(self):
        """Execute m_COMPTIME_Build_Message_Counters mapping"""
        try:
            self.logger.info("Starting m_COMPTIME_Build_Message_Counters")

            # Load source: U0287D01
            df_u0287d01 = self.spark.read.parquet("/data/sources/u0287d01")
            self.logger.info("Loaded source U0287D01: {} records".format(df_u0287d01.count()))

            # Source Qualifier: SQ_U0287D01
            # No additional processing needed for source qualifier

            # Expression: exp_Initial
            df_exp_initial = df.selectExpr(
                "SSN as SSN",
                "NAME as NAME",
                "case_when(TRUE,
			   is_number(SSN), 'D',
			  'NO') as o_RECORD_TYPE_FLAG"
            )

            # Filter: fil_Detail
            df_fil_detail = df.filter("RECORD_TYPE_FLAG = 'D'

--RECORD_TYPE_FLAG = 'H' or")

            # Aggregator: agg_ALL_RECORDS
            # Simple Aggregation
            df_agg = df.agg(
                F.count("SSN").alias("o_DETAIL_RECORD_COUNT")
            )
            

            # Expression: exp_Detail_Count
            df_exp_detail_count = df.selectExpr(
                "DETAIL_RECORD_COUNT as DETAIL_RECORD_COUNT",
                "'Y' as o_CURR_PP_FLAG"
            )

            # Lookup: lkp_PAY_PERIOD
            # Lookup table: PAY_PERIOD
            # Condition: CURR_PP_FLAG = in_CURR_PP_FLAG
            df_lkp_pay_period = df  # Lookup logic here

            # Expression: exp_Counters
            df_exp_counters = df.selectExpr(
                "'Number of detail records from the COMP TIME file.' as o_COUNTER_DESCRIPTION_1",
                "DETAIL_RECORD_COUNT as DETAIL_RECORD_COUNT",
                "lkp_PP_NUM as lkp_PP_NUM",
                "lkp_PP_END_YEAR as lkp_PP_END_YEAR"
            )

            # Expression: exp_Final
            df_exp_final = df.selectExpr(
                "SESSSTARTTIME as o_RUN_DATE",
                "$PMMappingName as o_PROCESS_NAME",
                "COUNTER_DESCRIPTION as COUNTER_DESCRIPTION",
                "COUNTER_VALUE as COUNTER_VALUE"
            )

            # Expression: exp_Build_Message
            df_exp_build_message = df.selectExpr(
                "when(PP_NUM < 10,
       LPAD(cast_str(PP_NUM), 2, '0'),
    cast_str(PP_NUM)
) as v_PP_NUM",
                "case_when(substring($PMRepositoryServiceName, 1, 4),
                       'Dev_', 'Dev: ',
                       'Test',   'Test: ',
                       'Prod',  'Prod: ')
 as v_ENVIRONMENT",
                "v_ENVIRONMENT ||
'Comp Time File loaded successfully for Pay Period:  ' || 
cast_str(PP_END_YEAR) || '-' || v_PP_NUM as v_SUBJECT",
                "SETVARIABLE($$MAP_SUBJECT, v_SUBJECT) as o_SUBJECT",
                "'Number of Detail Records from Comp Time file	= ' || cast_str(COUNTER_1) as v_MESSAGE",
                "SETVARIABLE($$MAP_MESSAGE, v_MESSAGE) as o_MESSAGE"
            )

            # Expression: exp_Final_Message
            df_exp_final_message = df.selectExpr(
                "SUBJECT as SUBJECT",
                "MESSAGE as MESSAGE"
            )

            # Write to target: COUNTER_TBL
            df_target.write.mode("overwrite").parquet("/data/targets/counter_tbl")
            self.logger.info("Written to target COUNTER_TBL")

            # Write to target: COMPTIME_MESSAGE_FILE
            df_target.write.mode("overwrite").parquet("/data/targets/comptime_message_file")
            self.logger.info("Written to target COMPTIME_MESSAGE_FILE")

            self.logger.info("Mapping completed successfully")
        except Exception as e:
            self.logger.error(f"Mapping failed: {str(e)}", exc_info=True)
            raise

    def _setup_logger(self):
        import logging
        logger = logging.getLogger(self.__class__.__name__)
        return logger