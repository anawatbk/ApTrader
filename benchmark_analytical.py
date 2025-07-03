#!/usr/bin/env python3
"""
Analytical Query Benchmark Script for DuckDB vs S3Client
Tests complex analytical queries to demonstrate DuckDB's strengths
"""

import time
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
from clients import S3StockDataClient, DuckDBStockClient


class AnalyticalBenchmark:
    """Benchmark analytical queries between DuckDB and S3Client"""
    
    def __init__(self):
        print("Initializing clients for analytical benchmarks...")
        self.s3_client = S3StockDataClient(bucket='anawatp-us-stocks')
        self.duck_client = DuckDBStockClient()
        
        # Test parameters
        self.test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        self.test_year = 2025
        self.start_date = '2025-03-12'
        self.end_date = '2025-03-20'  # ~9 days of data
        
    def benchmark_statistical_analysis(self) -> Dict[str, Any]:
        """Test multi-ticker statistical analysis"""
        print("\n=== Multi-Ticker Statistical Analysis ===")
        
        # DuckDB approach - complex analytical SQL
        print("  Running DuckDB statistical analysis...")
        start_time = time.time()
        
        sql_query = f"""
        SELECT 
            ticker,
            COUNT(*) as data_points,
            AVG(close) as avg_price,
            STDDEV(close) as price_volatility,
            AVG(volume) as avg_volume,
            MIN(low) as min_price,
            MAX(high) as max_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close) as median_price,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY close) as q1_price,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY close) as q3_price,
            CORR(close, volume) as price_volume_correlation
        FROM read_parquet([
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=AAPL/*.parquet',
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=MSFT/*.parquet',
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=GOOGL/*.parquet',
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=AMZN/*.parquet',
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=TSLA/*.parquet'
        ], hive_partitioning = 1)
        WHERE window_start_et::date BETWEEN '{self.start_date}'::date AND '{self.end_date}'::date
        GROUP BY ticker
        ORDER BY ticker
        """
        
        duck_result = self.duck_client.con.execute(sql_query).df()
        duck_time = time.time() - start_time
        
        # S3Client + Pandas approach
        print("  Running S3Client + pandas statistical analysis...")
        start_time = time.time()
        
        # Get raw data
        s3_data = self.s3_client.get_data(
            tickers=self.test_tickers,
            years=[self.test_year],
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # Perform equivalent pandas operations
        s3_result = s3_data.groupby('ticker').agg({
            'close': ['count', 'mean', 'std', 'median', 'quantile', 'min', 'max'],
            'volume': 'mean',
            'low': 'min',
            'high': 'max'
        }).round(4)
        
        # Calculate correlation for each ticker
        correlations = []
        for ticker in self.test_tickers:
            ticker_data = s3_data[s3_data['ticker'] == ticker]
            if len(ticker_data) > 1:
                corr = ticker_data['close'].corr(ticker_data['volume'])
                correlations.append(corr)
            else:
                correlations.append(np.nan)
        
        s3_time = time.time() - start_time
        
        return {
            'test_name': 'Statistical Analysis',
            'duck_time': duck_time,
            's3_time': s3_time,
            'duck_rows': len(duck_result),
            's3_rows': len(s3_result),
            'speedup': s3_time / duck_time if duck_time > 0 else 0
        }
    
    def benchmark_vwap_calculation(self) -> Dict[str, Any]:
        """Test Volume-Weighted Average Price calculation"""
        print("\n=== VWAP (Volume-Weighted Average Price) Analysis ===")
        
        # DuckDB approach
        print("  Running DuckDB VWAP calculation...")
        start_time = time.time()
        
        sql_query = f"""
        SELECT 
            ticker,
            window_start_et::date as trade_date,
            SUM(close * volume) / SUM(volume) as vwap,
            COUNT(*) as data_points,
            SUM(volume) as total_volume,
            MIN(low) as day_low,
            MAX(high) as day_high,
            (MAX(high) - MIN(low)) / AVG(close) as daily_range_pct
        FROM read_parquet([
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=AAPL/*.parquet',
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=MSFT/*.parquet',
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=GOOGL/*.parquet',
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=AMZN/*.parquet',
            's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=TSLA/*.parquet'
        ], hive_partitioning = 1)
        WHERE window_start_et::date BETWEEN '{self.start_date}'::date AND '{self.end_date}'::date
        GROUP BY ticker, window_start_et::date
        ORDER BY ticker, trade_date
        """
        
        duck_result = self.duck_client.con.execute(sql_query).df()
        duck_time = time.time() - start_time
        
        # S3Client + Pandas approach
        print("  Running S3Client + pandas VWAP calculation...")
        start_time = time.time()
        
        # Get raw data
        s3_data = self.s3_client.get_data(
            tickers=self.test_tickers,
            years=[self.test_year],
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # Add date column
        s3_data['trade_date'] = pd.to_datetime(s3_data['window_start_et']).dt.date
        
        # Calculate VWAP and other metrics
        s3_result = s3_data.groupby(['ticker', 'trade_date']).agg({
            'close': ['count', 'mean'],
            'volume': 'sum',
            'low': 'min',
            'high': 'max'
        })
        
        # Calculate VWAP manually
        vwap_data = []
        for (ticker, date), group in s3_data.groupby(['ticker', 'trade_date']):
            vwap = (group['close'] * group['volume']).sum() / group['volume'].sum()
            vwap_data.append({
                'ticker': ticker,
                'trade_date': date,
                'vwap': vwap
            })
        
        s3_time = time.time() - start_time
        
        return {
            'test_name': 'VWAP Calculation',
            'duck_time': duck_time,
            's3_time': s3_time,
            'duck_rows': len(duck_result),
            's3_rows': len(s3_result),
            'speedup': s3_time / duck_time if duck_time > 0 else 0
        }
    
    def benchmark_correlation_analysis(self) -> Dict[str, Any]:
        """Test cross-ticker correlation analysis"""
        print("\n=== Cross-Ticker Correlation Analysis ===")
        
        # DuckDB approach
        print("  Running DuckDB correlation analysis...")
        start_time = time.time()
        
        sql_query = f"""
        WITH daily_closes AS (
            SELECT 
                ticker,
                window_start_et::date as trade_date,
                AVG(close) as daily_close
            FROM read_parquet([
                's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=AAPL/*.parquet',
                's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=MSFT/*.parquet',
                's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=GOOGL/*.parquet',
                's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=AMZN/*.parquet',
                's3://anawatp-us-stocks/parquet/minute_aggs/year=2025/ticker=TSLA/*.parquet'
            ], hive_partitioning = 1)
            WHERE window_start_et::date BETWEEN '{self.start_date}'::date AND '{self.end_date}'::date
            GROUP BY ticker, window_start_et::date
        ),
        aapl_data AS (
            SELECT trade_date, daily_close as aapl_close 
            FROM daily_closes WHERE ticker = 'AAPL'
        )
        SELECT 
            dc.ticker,
            CORR(a.aapl_close, dc.daily_close) as correlation_with_aapl,
            COUNT(*) as data_points
        FROM daily_closes dc
        JOIN aapl_data a ON dc.trade_date = a.trade_date
        WHERE dc.ticker != 'AAPL'
        GROUP BY dc.ticker
        ORDER BY correlation_with_aapl DESC
        """
        
        duck_result = self.duck_client.con.execute(sql_query).df()
        duck_time = time.time() - start_time
        
        # S3Client + Pandas approach
        print("  Running S3Client + pandas correlation analysis...")
        start_time = time.time()
        
        # Get raw data
        s3_data = self.s3_client.get_data(
            tickers=self.test_tickers,
            years=[self.test_year],
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # Calculate daily closes
        s3_data['trade_date'] = pd.to_datetime(s3_data['window_start_et']).dt.date
        daily_closes = s3_data.groupby(['ticker', 'trade_date'])['close'].mean().reset_index()
        
        # Pivot to get correlation matrix
        pivot_data = daily_closes.pivot(index='trade_date', columns='ticker', values='close')
        
        # Calculate correlations with AAPL
        correlations = []
        if 'AAPL' in pivot_data.columns:
            for ticker in pivot_data.columns:
                if ticker != 'AAPL':
                    corr = pivot_data['AAPL'].corr(pivot_data[ticker])
                    correlations.append({
                        'ticker': ticker,
                        'correlation_with_aapl': corr,
                        'data_points': len(pivot_data.dropna())
                    })
        
        s3_time = time.time() - start_time
        
        return {
            'test_name': 'Correlation Analysis',
            'duck_time': duck_time,
            's3_time': s3_time,
            'duck_rows': len(duck_result),
            's3_rows': len(correlations),
            'speedup': s3_time / duck_time if duck_time > 0 else 0
        }
    
    def run_all_benchmarks(self) -> List[Dict[str, Any]]:
        """Run all analytical benchmarks"""
        print("Analytical Query Performance Benchmark")
        print("=" * 50)
        print(f"Test Period: {self.start_date} to {self.end_date}")
        print(f"Tickers: {', '.join(self.test_tickers)}")
        print(f"Year: {self.test_year}")
        
        results = []
        
        # Run statistical analysis benchmark
        try:
            result = self.benchmark_statistical_analysis()
            results.append(result)
            print(f"  DuckDB: {result['duck_time']:.2f}s ({result['duck_rows']} rows)")
            print(f"  S3Client: {result['s3_time']:.2f}s ({result['s3_rows']} rows)")
            if result['speedup'] > 1:
                print(f"  Winner: DuckDB is {result['speedup']:.1f}x faster")
            else:
                print(f"  Winner: S3Client is {1/result['speedup']:.1f}x faster")
        except Exception as e:
            print(f"  Error in statistical analysis: {e}")
        
        # Run VWAP benchmark
        try:
            result = self.benchmark_vwap_calculation()
            results.append(result)
            print(f"  DuckDB: {result['duck_time']:.2f}s ({result['duck_rows']} rows)")
            print(f"  S3Client: {result['s3_time']:.2f}s ({result['s3_rows']} rows)")
            if result['speedup'] > 1:
                print(f"  Winner: DuckDB is {result['speedup']:.1f}x faster")
            else:
                print(f"  Winner: S3Client is {1/result['speedup']:.1f}x faster")
        except Exception as e:
            print(f"  Error in VWAP calculation: {e}")
        
        # Run correlation benchmark
        try:
            result = self.benchmark_correlation_analysis()
            results.append(result)
            print(f"  DuckDB: {result['duck_time']:.2f}s ({result['duck_rows']} rows)")
            print(f"  S3Client: {result['s3_time']:.2f}s ({result['s3_rows']} rows)")
            if result['speedup'] > 1:
                print(f"  Winner: DuckDB is {result['speedup']:.1f}x faster")
            else:
                print(f"  Winner: S3Client is {1/result['speedup']:.1f}x faster")
        except Exception as e:
            print(f"  Error in correlation analysis: {e}")
        
        return results
    
    def generate_report(self, results: List[Dict[str, Any]]) -> None:
        """Generate analytical benchmark report"""
        print(f"\n{'='*60}")
        print("ANALYTICAL PERFORMANCE REPORT")
        print(f"{'='*60}")
        
        if not results:
            print("No successful benchmarks to analyze")
            return
        
        duck_times = [r['duck_time'] for r in results]
        s3_times = [r['s3_time'] for r in results]
        speedups = [r['speedup'] for r in results]
        
        print(f"\nSUMMARY:")
        print(f"  DuckDB Average: {np.mean(duck_times):.2f}s")
        print(f"  S3Client Average: {np.mean(s3_times):.2f}s")
        print(f"  Average Speedup: {np.mean(speedups):.1f}x")
        
        duck_wins = sum(1 for s in speedups if s > 1)
        s3_wins = len(speedups) - duck_wins
        
        print(f"\nWINNER BREAKDOWN:")
        print(f"  DuckDB wins: {duck_wins}/{len(results)} tests")
        print(f"  S3Client wins: {s3_wins}/{len(results)} tests")
        
        if np.mean(speedups) > 1:
            print(f"\n🚀 DuckDB is {np.mean(speedups):.1f}x faster on average for analytical queries!")
        else:
            print(f"\n📊 S3Client is {1/np.mean(speedups):.1f}x faster on average for analytical queries")


def main():
    """Run the analytical benchmark suite"""
    benchmark = AnalyticalBenchmark()
    results = benchmark.run_all_benchmarks()
    benchmark.generate_report(results)
    
    print(f"\n{'='*60}")
    print("ANALYTICAL BENCHMARK COMPLETE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()