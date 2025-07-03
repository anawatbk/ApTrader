#!/usr/bin/env python3
"""
Performance benchmark script for S3StockDataClient vs DuckDBStockClient
"""

import time
import pandas as pd
from typing import Dict, List, Any
from clients import S3StockDataClient, DuckDBStockClient


class ClientBenchmark:
    """Benchmark both stock data clients with identical queries"""
    
    def __init__(self):
        print("Initializing clients...")
        self.s3_client = S3StockDataClient(bucket='anawatp-us-stocks')
        self.duck_client = DuckDBStockClient()
        
    def benchmark_query(self, client, client_name: str, **query_params) -> Dict[str, Any]:
        """Benchmark a single query on a client"""
        print(f"  Running {client_name}...")
        
        start_time = time.time()
        try:
            result = client.get_data(**query_params)
            end_time = time.time()
            
            return {
                'client': client_name,
                'execution_time': end_time - start_time,
                'rows_returned': len(result),
                'success': True,
                'error': None
            }
        except Exception as e:
            end_time = time.time()
            return {
                'client': client_name,
                'execution_time': end_time - start_time,
                'rows_returned': 0,
                'success': False,
                'error': str(e)
            }
    
    def run_benchmark_suite(self) -> List[Dict[str, Any]]:
        """Run comprehensive benchmark suite"""
        results = []
        
        test_cases = [
            {
                'name': 'Small Query - Single Ticker, Single Day',
                'params': {
                    'tickers': ['AAPL'],
                    'years': [2025],
                    'start_date': '2025-03-12',
                    'end_date': '2025-03-12'
                }
            },
            {
                'name': 'Medium Query - Multiple Tickers, Single Day',
                'params': {
                    'tickers': ['AAPL', 'MSFT', 'GOOGL'],
                    'years': [2025],
                    'start_date': '2025-03-12',
                    'end_date': '2025-03-12'
                }
            },
            {
                'name': 'Large Query - Single Ticker, Multiple Days',
                'params': {
                    'tickers': ['AAPL'],
                    'years': [2025],
                    'start_date': '2025-03-12',
                    'end_date': '2025-03-20'
                }
            },
            {
                'name': 'Extra Large Query - Multiple Tickers, Multiple Days',
                'params': {
                    'tickers': ['AAPL', 'MSFT'],
                    'years': [2025],
                    'start_date': '2025-03-12',
                    'end_date': '2025-03-20'
                }
            },
            {
                'name': 'Column Selection - Specific Columns Only',
                'params': {
                    'tickers': ['AAPL'],
                    'years': [2025],
                    'start_date': '2025-03-12',
                    'end_date': '2025-03-12',
                    'columns': ['ticker', 'window_start_et', 'close', 'volume']
                }
            }
        ]
        
        for test_case in test_cases:
            print(f"\n=== {test_case['name']} ===")
            
            # Benchmark S3StockDataClient
            s3_result = self.benchmark_query(
                self.s3_client, 'S3StockDataClient', **test_case['params']
            )
            s3_result['test_case'] = test_case['name']
            results.append(s3_result)
            
            # Benchmark DuckDBStockClient
            duck_result = self.benchmark_query(
                self.duck_client, 'DuckDBStockClient', **test_case['params']
            )
            duck_result['test_case'] = test_case['name']
            results.append(duck_result)
            
            # Compare results
            if s3_result['success'] and duck_result['success']:
                speedup = s3_result['execution_time'] / duck_result['execution_time']
                faster_client = "DuckDB" if speedup > 1 else "S3"
                speed_diff = max(speedup, 1/speedup)
                
                print(f"  S3Client:    {s3_result['execution_time']:.2f}s ({s3_result['rows_returned']} rows)")
                print(f"  DuckDB:      {duck_result['execution_time']:.2f}s ({duck_result['rows_returned']} rows)")
                print(f"  Winner:      {faster_client} is {speed_diff:.1f}x faster")
                
                if s3_result['rows_returned'] != duck_result['rows_returned']:
                    print(f"  ⚠️  WARNING: Row count mismatch!")
            else:
                print(f"  ❌ Error occurred")
                if not s3_result['success']:
                    print(f"     S3Client error: {s3_result['error']}")
                if not duck_result['success']:
                    print(f"     DuckDB error: {duck_result['error']}")
        
        return results
    
    def generate_report(self, results: List[Dict[str, Any]]) -> None:
        """Generate performance analysis report"""
        print(f"\n{'='*60}")
        print("PERFORMANCE ANALYSIS REPORT")
        print(f"{'='*60}")
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame(results)
        successful_results = df[df['success'] == True]
        
        if len(successful_results) == 0:
            print("❌ No successful benchmarks to analyze")
            return
        
        # Group by test case for comparison
        for test_case in successful_results['test_case'].unique():
            test_data = successful_results[successful_results['test_case'] == test_case]
            
            if len(test_data) == 2:  # Both clients completed
                s3_time = test_data[test_data['client'] == 'S3StockDataClient']['execution_time'].iloc[0]
                duck_time = test_data[test_data['client'] == 'DuckDBStockClient']['execution_time'].iloc[0]
                
                speedup = s3_time / duck_time
                faster_client = "DuckDB" if speedup > 1 else "S3"
                speed_factor = max(speedup, 1/speedup)
                
                print(f"\n{test_case}:")
                print(f"  S3Client:  {s3_time:.2f}s")
                print(f"  DuckDB:    {duck_time:.2f}s")
                print(f"  Result:    {faster_client} is {speed_factor:.1f}x faster")
        
        # Overall statistics
        s3_times = successful_results[successful_results['client'] == 'S3StockDataClient']['execution_time']
        duck_times = successful_results[successful_results['client'] == 'DuckDBStockClient']['execution_time']
        
        if len(s3_times) > 0 and len(duck_times) > 0:
            print(f"\nOVERALL STATISTICS:")
            print(f"  S3Client Average:    {s3_times.mean():.2f}s")
            print(f"  DuckDB Average:      {duck_times.mean():.2f}s")
            print(f"  S3Client Range:      {s3_times.min():.2f}s - {s3_times.max():.2f}s")
            print(f"  DuckDB Range:        {duck_times.min():.2f}s - {duck_times.max():.2f}s")
            
            avg_speedup = s3_times.mean() / duck_times.mean()
            if avg_speedup > 1:
                print(f"  DuckDB is {avg_speedup:.1f}x faster on average")
            else:
                print(f"  S3Client is {1/avg_speedup:.1f}x faster on average")


def main():
    """Run the benchmark suite"""
    print("Stock Data Client Performance Benchmark")
    print("=" * 50)
    
    benchmark = ClientBenchmark()
    results = benchmark.run_benchmark_suite()
    benchmark.generate_report(results)
    
    print(f"\n{'='*60}")
    print("OPTIMIZATION RECOMMENDATIONS")
    print(f"{'='*60}")
    
    # Analyze results for optimization suggestions
    df = pd.DataFrame(results)
    successful_results = df[df['success'] == True]
    
    if len(successful_results) > 0:
        duck_times = successful_results[successful_results['client'] == 'DuckDBStockClient']['execution_time']
        s3_times = successful_results[successful_results['client'] == 'S3StockDataClient']['execution_time']
        
        if len(duck_times) > 0 and len(s3_times) > 0:
            avg_speedup = s3_times.mean() / duck_times.mean()
            
            if avg_speedup < 1:  # S3 is faster
                print("\n🐌 DuckDB is slower than S3Client. Recommended optimizations:")
                print("1. Implement smart partition targeting (avoid year=*/ticker=* wildcards)")
                print("2. Increase DuckDB memory limits and parallel threads")
                print("3. Use specific S3 paths based on query filters")
                print("4. Enable DuckDB S3 transfer optimizations")
                print("5. Add explicit partition predicates to queries")
            else:
                print("\n🚀 DuckDB is faster! Consider using it as the default client.")
    
    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()