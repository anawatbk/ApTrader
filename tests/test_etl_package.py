import os
import sys
import unittest


class TestETLPackage(unittest.TestCase):
    """Test that the ETL package can be imported correctly"""

    def test_etl_package_import(self):
        """Test that the etl package can be imported"""
        import etl
        self.assertTrue(hasattr(etl, '__version__'))
        self.assertEqual(etl.__version__, "1.0.0")

    def test_glue_module_import(self):
        """Test that the Glue ETL module can be imported"""
        from etl import s3_csv_to_s3_parquet_glue_shell_job
        
        # Check that key functions exist
        self.assertTrue(hasattr(s3_csv_to_s3_parquet_glue_shell_job, 'convert_csv_to_parquet'))
        self.assertTrue(hasattr(s3_csv_to_s3_parquet_glue_shell_job, 'get_existing_partitions'))
        self.assertTrue(hasattr(s3_csv_to_s3_parquet_glue_shell_job, 'filter_dataframe_by_missing_tickers'))
        self.assertTrue(hasattr(s3_csv_to_s3_parquet_glue_shell_job, 'main'))

    def test_etl_functions_available(self):
        """Test that ETL functions are available through package imports"""
        import etl
        
        # These should be available (might be None if import fails, but should exist)
        self.assertTrue(hasattr(etl, 'glue_convert_csv_to_parquet'))
        self.assertTrue(hasattr(etl, 'get_existing_partitions'))
        self.assertTrue(hasattr(etl, 'filter_dataframe_by_missing_tickers'))
        self.assertTrue(hasattr(etl, 'glue_main'))

    def test_glue_available_flag(self):
        """Test that the GLUE_AVAILABLE flag works correctly"""
        from etl import s3_csv_to_s3_parquet_glue_shell_job
        
        # Should have the GLUE_AVAILABLE flag
        self.assertTrue(hasattr(s3_csv_to_s3_parquet_glue_shell_job, 'GLUE_AVAILABLE'))
        # In local environment, should be False
        self.assertFalse(s3_csv_to_s3_parquet_glue_shell_job.GLUE_AVAILABLE)

    def test_mock_glue_function(self):
        """Test that the mock getResolvedOptions function works"""
        from etl import s3_csv_to_s3_parquet_glue_shell_job
        
        # Test the mock function
        result = s3_csv_to_s3_parquet_glue_shell_job.getResolvedOptions(['script.py', '2025'], ['JOB_NAME', 'YEAR'])
        self.assertEqual(result['YEAR'], '2025')
        self.assertEqual(result['JOB_NAME'], 'local-test')


if __name__ == '__main__':
    unittest.main(verbosity=2)