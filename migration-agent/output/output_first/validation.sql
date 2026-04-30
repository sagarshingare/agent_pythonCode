-- Validation Queries for: m_COMPTIME_Build_Message_Counters
-- Description: This mapping gets the count of detail records on the CompTime file that was processed and loads it to the Counters Table. 

-- Row Count Checks

SELECT COUNT(*) as row_count FROM source.U0287D01;
SELECT COUNT(*) as row_count FROM target.COUNTER_TBL;
SELECT COUNT(*) as row_count FROM target.COMPTIME_MESSAGE_FILE;

-- Data Reconciliation (Source vs Target)

SELECT SSN, NAME, CURRENT_ACCT, CURRENT_ORG, FLSA_STATUS, COMP_TIME_CUR_BAL, COMP_TIME_YEAR_EARNED, PP_END_DATE, DAILY_DATE_EARNED, COMP_TIME_RATE, COMP_TIME_HOURS, COMP_TIME_UNDEF FROM source.U0287D01
EXCEPT
SELECT RUN_DATE, PROCESS_NAME, COUNTER_DESCRIPTION, COUNTER_VALUE, PP_END_YEAR, PP_NUM, CYCLE_ID FROM target.COUNTER_TBL;

-- Hash Validation for Data Integrity

SELECT
    MD5(CONCAT(RUN_DATE, PROCESS_NAME, COUNTER_DESCRIPTION, COUNTER_VALUE, PP_END_YEAR, PP_NUM, CYCLE_ID)) as data_hash,
    COUNT(*) as record_count
FROM target.COUNTER_TBL
GROUP BY data_hash;

SELECT
    MD5(CONCAT(SUBJECT, MESSAGE)) as data_hash,
    COUNT(*) as record_count
FROM target.COMPTIME_MESSAGE_FILE
GROUP BY data_hash;


-- Validation Queries for: m_COMPTIME_Load_COMP_TIME_DAILY_TBL
-- Description: 

-- Row Count Checks

SELECT COUNT(*) as row_count FROM source.U0287D01;
SELECT COUNT(*) as row_count FROM target.COMP_TIME_DAILY_TBL;

-- Data Reconciliation (Source vs Target)

SELECT SSN, NAME, CURRENT_ACCT, CURRENT_ORG, FLSA_STATUS, COMP_TIME_CUR_BAL, COMP_TIME_YEAR_EARNED, PP_END_DATE, DAILY_DATE_EARNED, COMP_TIME_RATE, COMP_TIME_HOURS, COMP_TIME_UNDEF FROM source.U0287D01
EXCEPT
SELECT PP_END_YEAR, PP_NUM, PP_YEAR_NUM, SSN, NAME, CURRENT_ACCT, CURRENT_ORG, FLSA_STATUS, COMP_TIME_CUR_BAL, COMP_TIME_YEAR_EARNED, PP_END_DATE, DAILY_DATE_EARNED, COMP_TIME_RATE, COMP_TIME_HOURS, COMP_TIME_UNDEF FROM target.COMP_TIME_DAILY_TBL;

-- Hash Validation for Data Integrity

SELECT
    MD5(CONCAT(PP_END_YEAR, PP_NUM, PP_YEAR_NUM, SSN, NAME, CURRENT_ACCT, CURRENT_ORG, FLSA_STATUS, COMP_TIME_CUR_BAL, COMP_TIME_YEAR_EARNED, PP_END_DATE, DAILY_DATE_EARNED, COMP_TIME_RATE, COMP_TIME_HOURS, COMP_TIME_UNDEF)) as data_hash,
    COUNT(*) as record_count
FROM target.COMP_TIME_DAILY_TBL
GROUP BY data_hash;


-- Validation Queries for: m_COMPTIME_Current_Pay_Period
-- Description: This mapping returns the Current Pay Period from the Pay Period table.

-- Row Count Checks

SELECT COUNT(*) as row_count FROM source.PAY_PERIOD;
SELECT COUNT(*) as row_count FROM target.COMP_TIME_DATE_FILE;

-- Data Reconciliation (Source vs Target)

SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, LV_NUM, LV_YEAR, PAY_DTE, CURR_PP_FLAG, HOLIDAY_1, HOLIDAY_2 FROM source.PAY_PERIOD
EXCEPT
SELECT PAY_PERIOD FROM target.COMP_TIME_DATE_FILE;

-- Hash Validation for Data Integrity

SELECT
    MD5(CONCAT(PAY_PERIOD)) as data_hash,
    COUNT(*) as record_count
FROM target.COMP_TIME_DATE_FILE
GROUP BY data_hash;
