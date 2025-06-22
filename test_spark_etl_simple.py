#!/usr/bin/env python3
"""
Simple test script for Spark ETL functionality without pytest complications
"""

import sys
import os
from datetime import datetime

# Add etl to path
sys.path.append('etl')

def test_imports():
    """Test that all imports work without try-catch blocks"""
    print("Testing imports...")
    
    # Import PySpark
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    print("✓ PySpark imports successful")
    
    # Import our ETL module
    import s3_csv_to_s3_parquet_spark_job as spark_etl_module
    print("✓ Spark ETL module import successful")
    
    # Check availability flags
    print(f"✓ GLUE_AVAILABLE: {spark_etl_module.GLUE_AVAILABLE}")
    print(f"✓ SPARK_AVAILABLE: {spark_etl_module.SPARK_AVAILABLE}")
    
    return spark_etl_module

def test_spark_session_creation(spark_etl_module):
    """Test creating a Spark session locally"""
    print("\nTesting Spark session creation...")
    
    spark, glue_context, job = spark_etl_module.create_spark_session("test_app")
    
    assert spark is not None, "SparkSession should not be None"
    assert glue_context is None, "GlueContext should be None in local mode"
    assert job is None, "Job should be None in local mode"
    assert spark.sparkContext.appName == "test_app", f"Expected app name 'test_app', got '{spark.sparkContext.appName}'"
    
    print(f"✓ Spark version: {spark.version}")
    print(f"✓ App name: {spark.sparkContext.appName}")
    
    spark.stop()
    print("✓ Spark session created and stopped successfully")

def test_trading_hours_filtering(spark_etl_module):
    """Test trading hours filtering logic"""
    print("\nTesting trading hours filtering...")
    
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    
    # Create fresh Spark session
    spark = SparkSession.builder \
        .appName("test_trading_hours") \
        .master("local[2]") \
        .getOrCreate()
    
    # Test data with various timestamps
    test_data = [
        ("AAPL", datetime(2025, 1, 15, 9, 0, 0)),   # Before market open
        ("AAPL", datetime(2025, 1, 15, 9, 30, 0)),  # Market open - should be included
        ("AAPL", datetime(2025, 1, 15, 12, 0, 0)),  # Midday - should be included
        ("AAPL", datetime(2025, 1, 15, 15, 59, 0)), # Before market close - should be included
        ("AAPL", datetime(2025, 1, 15, 16, 0, 0)),  # Market close - should be excluded
        ("AAPL", datetime(2025, 1, 15, 20, 0, 0)),  # After hours - should be excluded
        ("AAPL", datetime(2025, 1, 18, 12, 0, 0)),  # Saturday - should be excluded
        ("AAPL", datetime(2025, 1, 19, 12, 0, 0)),  # Sunday - should be excluded
    ]
    
    # Create DataFrame
    schema = T.StructType([
        T.StructField("ticker", T.StringType(), True),
        T.StructField("window_start_et", T.TimestampType(), True)
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Apply trading hours filter
    filtered_df = df.filter(
        # Between 9:30 AM and 4:00 PM ET on weekdays
        (
            ((F.hour(F.col("window_start_et")) == 9) & (F.minute(F.col("window_start_et")) >= 30)) |
            ((F.hour(F.col("window_start_et")) > 9) & (F.hour(F.col("window_start_et")) < 16))
        ) &
        (F.dayofweek(F.col("window_start_et")).between(2, 6))  # Monday=2, Friday=6
    )
    
    result = filtered_df.collect()
    
    # Should only include 3 rows: 09:30, 12:00, 15:59 on weekday
    assert len(result) == 3, f"Expected 3 results, got {len(result)}"
    
    # Verify the correct timestamps are included
    timestamps = [row.window_start_et.strftime("%H:%M") for row in result]
    expected_times = ["09:30", "12:00", "15:59"]
    
    assert sorted(timestamps) == sorted(expected_times), f"Expected {expected_times}, got {timestamps}"
    
    print(f"✓ Trading hours filtering successful: {timestamps}")
    
    spark.stop()

def test_configuration_constants(spark_etl_module):
    """Test configuration constants"""
    print("\nTesting configuration constants...")
    
    assert hasattr(spark_etl_module, 'S3_CSV_INPUT_TEMPLATE'), "S3_CSV_INPUT_TEMPLATE should exist"
    assert hasattr(spark_etl_module, 'PARQUET_OUTPUT_PATH'), "PARQUET_OUTPUT_PATH should exist"
    
    # Verify the paths use s3a:// protocol for Spark
    assert spark_etl_module.S3_CSV_INPUT_TEMPLATE.startswith('s3a://'), "Should use s3a:// protocol"
    assert spark_etl_module.PARQUET_OUTPUT_PATH.startswith('s3a://'), "Should use s3a:// protocol"
    
    print(f"✓ S3 CSV input template: {spark_etl_module.S3_CSV_INPUT_TEMPLATE}")
    print(f"✓ Parquet output path: {spark_etl_module.PARQUET_OUTPUT_PATH}")

def main():
    """Run all tests"""
    print("=" * 60)
    print("SPARK ETL SIMPLE TESTS")
    print("=" * 60)
    
    try:
        # Test imports
        spark_etl_module = test_imports()
        
        # Test Spark session creation
        test_spark_session_creation(spark_etl_module)
        
        # Test trading hours filtering
        test_trading_hours_filtering(spark_etl_module)
        
        # Test configuration constants
        test_configuration_constants(spark_etl_module)
        
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED! ✓")
        print("Spark ETL functionality is working correctly without try-catch blocks")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()