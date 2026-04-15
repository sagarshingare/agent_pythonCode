from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from comptime.comptime_etl import CompTimeETL

BASE_DIR = Path(__file__).resolve().parent


@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local[1]").appName("CompTimeETLTest").getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def etl_config(tmp_path):
    return {
        'comptime_source_path': str(BASE_DIR / 'sample_comptime_data.csv'),
        'pay_period_path': str(BASE_DIR / 'sample_pay_period.csv'),
        'comp_time_daily_target': str(tmp_path / 'comp_time_daily_tbl'),
        'counter_target': str(tmp_path / 'counter_tbl'),
        'message_file_target': str(tmp_path / 'comptime_message_file.txt'),
        'date_file_target': str(tmp_path / 'comp_time_date_file.txt'),
        'repartition_count': 1,
        'batch_size': 10000
    }


def test_comptime_etl_run_success(spark, etl_config):
    etl_processor = CompTimeETL(spark, etl_config)
    assert etl_processor.run_etl() is True

    assert Path(etl_config['comp_time_daily_target']).exists()
    assert Path(etl_config['counter_target']).exists()
    assert Path(etl_config['message_file_target']).is_file()
    assert Path(etl_config['date_file_target']).is_file()


def test_get_current_pay_period(spark, etl_config):
    etl_processor = CompTimeETL(spark, etl_config)
    pay_period_info = etl_processor.get_current_pay_period()

    assert pay_period_info['PP_NUM'] == 12
    assert pay_period_info['PP_END_YEAR'] == 2023
    assert pay_period_info['PP_YEAR_NUM'] == '202312'


def test_validate_comptime_data(spark, etl_config):
    etl_processor = CompTimeETL(spark, etl_config)
    comptime_df = spark.read.option('header', 'false').schema(etl_processor.comptime_source_schema).csv(etl_config['comptime_source_path'])
    validated_df = etl_processor.validate_comptime_data(comptime_df)

    assert 'VALID_RECORD_FLAG' in validated_df.columns
    assert validated_df.filter(validated_df.VALID_RECORD_FLAG == 0).count() == 0
