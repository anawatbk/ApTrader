# ApTrader

Algorithmic trading analysis and data processing tools for downloading and processing stock data from S3 with year/ticker partitioning.

## Features

- **S3 Stock Data Client**: Download partitioned stock data from S3 in parquet format
- **Query Builder**: Flexible filtering by tickers, date ranges, and columns
- **ETL Tools**: Process and transform stock data
- **Data Handlers**: Abstract interfaces for market data fetching
- **Bulk Downloaders**: Efficiently download large datasets

## Quick Start

```python
from clients import S3StockDataClient

# Initialize client
client = S3StockDataClient(bucket='my-stock-data-bucket')

# Get data for specific tickers and date range
df = client.get_data(
    tickers=['AAPL', 'MSFT'], 
    start_date='2024-01-01', 
    end_date='2024-12-31'
)

# Stream large datasets
for chunk in client.stream_data(ticker='AAPL', year=2024):
    process(chunk)
```

## Development Setup

### Prerequisites

- Python 3.13.0 or higher
- pyenv (recommended for Python version management)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ApTrader
   ```

2. **Set up Python environment**
   ```bash
   # Using pyenv (recommended)
   pyenv install 3.13.0
   pyenv virtualenv 3.13.0 ApTraderEnv
   pyenv local ApTraderEnv
   
   # Or using venv
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install the package in development mode**
   ```bash
   # Install core dependencies
   pip install -e .
   
   # Install with development dependencies
   pip install -e ".[dev]"
   
   # Install all optional dependencies
   pip install -e ".[all]"
   ```

### Development Workflow

#### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_s3_stock_client.py

# Run specific test class
pytest tests/test_s3_stock_client.py::TestS3StockDataClient

# Run specific test method
pytest tests/test_s3_stock_client.py::TestQueryBuilder::test_with_tickers_single

# Run tests with coverage
pytest --cov=clients --cov=data_handler --cov=etl --cov=utils

# Run tests and generate HTML coverage report
pytest --cov=clients --cov-report=html
```

#### Code Quality

```bash
# Format code with black
black .

# Sort imports with isort
isort .

# Lint with flake8
flake8 .

# Type checking with mypy
mypy clients/ data_handler/ etl/ utils/
```

#### Working with Jupyter Notebooks

```bash
# Install jupyter dependencies
pip install -e ".[jupyter]"

# Start Jupyter Lab
jupyter lab

# Or Jupyter Notebook
jupyter notebook
```

### Project Structure

```
ApTrader/
├── clients/                 # S3 stock data clients
│   ├── __init__.py
│   ├── s3_stock_client.py  # Main S3 client
│   ├── query_builder.py    # Query filtering
│   └── exceptions.py       # Custom exceptions
├── data_handler/           # Abstract data handlers
├── etl/                    # ETL scripts and tools
├── utils/                  # Utility functions
├── bulk_downloader/        # Bulk download tools
├── tests/                  # Test suite
├── notebooks/              # Jupyter notebooks
├── docs/                   # Documentation
├── config.toml            # Configuration file
├── pyproject.toml         # Package configuration
└── requirements.txt       # Dependencies
```


### Running ETL Scripts

```bash
# Example: Convert CSV to Parquet
python etl/s3_csv_to_s3_parquet_job.py

# Example: Local CSV to S3
python etl/local_csv_to_s3.py

# Deploy and run AWS Glue ETL jobs
./shell_scripts/deploy_glue_etl.sh deploy my-bucket-name
./shell_scripts/deploy_glue_etl.sh run 2024
./shell_scripts/deploy_glue_etl.sh monitor jr_1234567890abcdef
```

### Adding New Tests

1. Create test files in the `tests/` directory with the naming pattern `test_*.py`
2. Use pytest fixtures for common setup
3. Mock external dependencies (S3, APIs, etc.)
4. Follow the existing test patterns:

```python
import pytest
from unittest.mock import Mock, patch
from clients.s3_stock_client import S3StockDataClient

class TestMyNewFeature:
    @pytest.fixture
    def sample_config(self):
        return {'bucket': 'test-bucket'}
    
    @patch('clients.s3_stock_client.s3fs.S3FileSystem')
    def test_my_feature(self, mock_s3fs, sample_config):
        # Test implementation
        pass
```

### Troubleshooting

#### Import Errors
If you see "Unresolved reference" errors in your IDE:
```bash
# Reinstall in editable mode
pip install -e .

# Verify installation
pip show aptrader
python -c "from clients import S3StockDataClient; print('Success!')"
```

#### Python Environment Issues
```bash
# Check current Python version
python --version

# Check pyenv version
pyenv version

# Recreate environment if needed
pyenv virtualenv 3.13.0 ApTraderEnv-new
pyenv local ApTraderEnv-new
pip install -e ".[all]"
```

#### Test Failures
```bash
# Run tests with more verbose output
pytest -vvv --tb=long

# Run specific failing test
pytest tests/test_s3_stock_client.py::TestS3StockDataClient::test_specific_method -vvv
```

### Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the test suite: `pytest`
5. Format your code: `black . && isort .`
6. Commit your changes: `git commit -m 'Add amazing feature'`
7. Push to the branch: `git push origin feature/amazing-feature`
8. Open a Pull Request

### License

This project is licensed under the MIT License - see the LICENSE file for details.
