#!/usr/bin/env python3
"""
Quick test script for the new ticker-only list_partitions feature
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from clients.s3_stock_client import S3StockDataClient

def test_ticker_only_feature():
    """Test the new ticker-only list_partitions feature"""
    print("Testing ticker-only list_partitions feature...")
    
    try:
        # Initialize client
        client = S3StockDataClient(
            bucket="anawatp-us-stocks",
            base_prefix="parquet",
            aws_region="us-west-2"
        )
        
        print("✅ S3 client initialized successfully")
        
        # Test 1: List all AAPL partitions across years
        print("\n=== Test 1: List AAPL partitions across all years ===")
        aapl_partitions = client.list_partitions(ticker="AAPL")
        print(f"Found {len(aapl_partitions)} AAPL partitions:")
        for p in aapl_partitions[:5]:  # Show first 5
            print(f"  {p['year']}: {p['files']} files")
        
        # Test 2: Case insensitive
        print("\n=== Test 2: Case insensitive ticker ===")
        aapl_lower = client.list_partitions(ticker="aapl")
        print(f"Found {len(aapl_lower)} partitions for 'aapl' (lowercase)")
        assert len(aapl_lower) == len(aapl_partitions), "Case sensitivity issue!"
        
        # Test 3: Non-existent ticker
        print("\n=== Test 3: Non-existent ticker ===")
        fake_partitions = client.list_partitions(ticker="NONEXISTENT")
        print(f"Found {len(fake_partitions)} partitions for 'NONEXISTENT'")
        assert len(fake_partitions) == 0, "Should return empty list for non-existent ticker"
        
        # Test 4: Compare with year+ticker filtering
        if aapl_partitions:
            year = aapl_partitions[0]['year']
            print(f"\n=== Test 4: Compare with year+ticker filtering (year={year}) ===")
            specific_partition = client.list_partitions(year=year, ticker="AAPL")
            print(f"Found {len(specific_partition)} partitions for AAPL in {year}")
            
        print("\n✅ All tests passed! Ticker-only feature is working correctly.")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = test_ticker_only_feature()
    sys.exit(0 if success else 1)