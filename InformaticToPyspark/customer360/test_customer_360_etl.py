"""
Unit tests for Customer360ETL PySpark implementation.

Tests cover:
- Data reading and schema validation
- Derived field calculations
- Record validation and filtering
- Aggregation logic
- Schema validation
- Error handling
"""

import unittest
import tempfile
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, DateType, TimestampType

from customer_360_etl import Customer360ETL


class TestCustomer360ETL(unittest.TestCase):
    """Test cases for Customer360ETL class."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures before running tests."""
        cls.spark = SparkSession.builder \
            .appName("TestCustomer360ETL") \
            .master("local[*]") \
            .getOrCreate()

        # Create temporary directory for test files
        cls.test_dir = tempfile.mkdtemp()

        # Create test data
        cls.create_test_data()

    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures after running tests."""
        cls.spark.stop()
        shutil.rmtree(cls.test_dir)

    @classmethod
    def create_test_data(cls):
        """Create test CSV data for testing."""
        test_data = """CUSTOMER_ID,ACCOUNT_ID,TXN_AMOUNT,TXN_DATE,LAST_UPDATED_TS
1,1001,50000.00,2023-01-15,2023-01-15 10:00:00
1,1001,75000.00,2023-02-20,2023-02-20 11:00:00
1,1001,,2023-03-10,2023-03-10 12:00:00
2,1002,25000.00,2023-01-05,2023-01-05 09:00:00
2,1002,80000.00,2023-03-25,2023-03-25 13:00:00
3,1003,150000.00,2023-01-10,2023-01-10 14:00:00"""

        test_file = Path(cls.test_dir) / "test_source.csv"
        with open(test_file, 'w') as f:
            f.write(test_data)

        cls.test_source_path = str(test_file)

    def setUp(self):
        """Set up test case."""
        self.config = {
            'source_path': self.test_source_path,
            'target_path': str(Path(self.test_dir) / "test_target"),
            'last_successful_run': '2023-01-01 00:00:00',
            'high_txn_threshold': 100000.0,
            'partition_by': ['CUSTOMER_ID'],
            'repartition_count': 2
        }
        self.etl = Customer360ETL(self.spark, self.config)

    def test_initialization(self):
        """Test ETL initialization."""
        self.assertIsNotNone(self.etl.spark)
        self.assertIsNotNone(self.etl.config)
        self.assertIsNotNone(self.etl.source_schema)
        self.assertIsNotNone(self.etl.target_schema)

    def test_config_validation(self):
        """Test configuration validation."""
        # Valid config should pass
        self.etl.validate_config()

        # Invalid config should raise error
        invalid_config = {'source_path': 'test.csv'}  # Missing required keys
        etl_invalid = Customer360ETL(self.spark, invalid_config)
        with self.assertRaises(ValueError):
            etl_invalid.validate_config()

    def test_read_source_data(self):
        """Test reading source data with incremental logic."""
        df = self.etl.read_source_data()

        # Should read 6 records (all after 2023-01-01)
        self.assertEqual(df.count(), 6)

        # Check schema
        expected_columns = ['CUSTOMER_ID', 'ACCOUNT_ID', 'TXN_AMOUNT', 'TXN_DATE', 'LAST_UPDATED_TS']
        self.assertEqual(df.columns, expected_columns)

    def test_add_derived_fields(self):
        """Test adding derived fields."""
        source_df = self.etl.read_source_data()
        derived_df = self.etl.add_derived_fields(source_df)

        # Should have new columns
        self.assertIn('TXN_FLAG', derived_df.columns)
        self.assertIn('VALID_TXN', derived_df.columns)

        # Test TXN_FLAG logic
        high_txn_row = derived_df.filter(derived_df.CUSTOMER_ID == 3).first()
        self.assertEqual(high_txn_row.TXN_FLAG, 'HIGH')

        normal_txn_rows = derived_df.filter(derived_df.CUSTOMER_ID != 3)
        for row in normal_txn_rows.collect():
            self.assertEqual(row.TXN_FLAG, 'NORMAL')

        # Test VALID_TXN logic
        valid_rows = derived_df.filter(derived_df.VALID_TXN == 1)
        invalid_rows = derived_df.filter(derived_df.VALID_TXN == 0)

        self.assertEqual(valid_rows.count(), 5)  # 5 valid records
        self.assertEqual(invalid_rows.count(), 1)  # 1 invalid record (null TXN_AMOUNT)

    def test_validate_and_filter_records(self):
        """Test record validation and filtering."""
        source_df = self.etl.read_source_data()
        derived_df = self.etl.add_derived_fields(source_df)
        valid_df = self.etl.validate_and_filter_records(derived_df)

        # Should filter out invalid records
        self.assertEqual(valid_df.count(), 5)

        # All remaining records should be valid
        valid_check = valid_df.filter(valid_df.VALID_TXN == 1).count()
        self.assertEqual(valid_check, 5)

    def test_aggregate_transactions(self):
        """Test transaction aggregation."""
        source_df = self.etl.read_source_data()
        derived_df = self.etl.add_derived_fields(source_df)
        valid_df = self.etl.validate_and_filter_records(derived_df)
        agg_df = self.etl.aggregate_transactions(valid_df)

        # Should have 3 customers (1, 2, 3)
        self.assertEqual(agg_df.count(), 3)

        # Check aggregation results
        customer_1 = agg_df.filter(agg_df.CUSTOMER_ID == 1).first()
        self.assertEqual(customer_1.TOTAL_TXN, 125000.0)  # 50000 + 75000
        self.assertEqual(customer_1.MAX_TXN, 75000.0)

        customer_2 = agg_df.filter(agg_df.CUSTOMER_ID == 2).first()
        self.assertEqual(customer_2.TOTAL_TXN, 105000.0)  # 25000 + 80000

        customer_3 = agg_df.filter(agg_df.CUSTOMER_ID == 3).first()
        self.assertEqual(customer_3.TOTAL_TXN, 150000.0)
        self.assertEqual(customer_3.MAX_TXN, 150000.0)

        # Check LOAD_DT column exists
        self.assertIn('LOAD_DT', agg_df.columns)

    def test_target_schema_validation(self):
        """Test target schema validation."""
        source_df = self.etl.read_source_data()
        derived_df = self.etl.add_derived_fields(source_df)
        valid_df = self.etl.validate_and_filter_records(derived_df)
        agg_df = self.etl.aggregate_transactions(valid_df)

        # Should pass validation
        self.etl.validate_target_schema(agg_df)

        # Test with invalid schema
        invalid_df = agg_df.drop('CUSTOMER_ID')
        with self.assertRaises(ValueError):
            self.etl.validate_target_schema(invalid_df)

    def test_write_target_data(self):
        """Test writing target data."""
        source_df = self.etl.read_source_data()
        derived_df = self.etl.add_derived_fields(source_df)
        valid_df = self.etl.validate_and_filter_records(derived_df)
        agg_df = self.etl.aggregate_transactions(valid_df)

        # Write data
        self.etl.write_target_data(agg_df)

        # Verify data was written
        target_path = Path(self.config['target_path'])
        self.assertTrue(target_path.exists())

        # Read back and verify
        read_df = self.spark.read.parquet(str(target_path))
        self.assertEqual(read_df.count(), 3)

    def test_full_etl_pipeline(self):
        """Test the complete ETL pipeline."""
        success = self.etl.run_etl()
        self.assertTrue(success)

        # Verify output
        target_path = Path(self.config['target_path'])
        self.assertTrue(target_path.exists())

        result_df = self.spark.read.parquet(str(target_path))
        self.assertEqual(result_df.count(), 3)


if __name__ == '__main__':
    unittest.main()