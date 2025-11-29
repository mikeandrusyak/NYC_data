# NYC Data Analysis & Wrangling Project

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive data analysis pipeline for processing NYC 311 service requests and median rent data. This project demonstrates modular architecture, automated testing, and proper package management.

## 📊 Project Overview

This project analyzes the relationship between NYC 311 complaint patterns and median rent prices across different neighborhoods. It provides:

- **Automated data collection** from NYC Open Data API and StreetEasy
- **ETL pipeline** for cleaning, transforming, and integrating multiple data sources
- **Geographic analysis** using UHF (United Hospital Fund) neighborhood boundaries
- **Statistical insights** on complaint resolution times and rental market trends
- **Visualization tools** for exploratory data analysis

### Key Features

- ✅ **Installable Python package** with proper dependency management
- ✅ **Command-line interface** for running data pipelines
- ✅ **Modular architecture** with separated concerns (load, clean, transform, integrate)
- ✅ **Comprehensive test suite** with 95%+ code coverage
- ✅ **Type hints** throughout the codebase
- ✅ **Configurable settings** with sensible defaults

---

## 🚀 Quick Start

### Prerequisites

- Python 3.12 or higher
- Git
- Virtual environment (recommended)

### Installation

1. **Clone the repository:**
```bash
git clone https://gitlab.fhnw.ch/roberto.fazekas/DAW_NYC_data.git
cd RAW_NYC_data
```

2. **Create and activate virtual environment:**
```bash
python3.12 -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# .venv\Scripts\activate    # On Windows
```

3. **Install the package:**
```bash
pip install -e .
```

This will install:
- The `daw_nyc` package in editable mode
- All required dependencies (pandas, numpy, matplotlib, etc.)
- Development tools (pytest, coverage) if using `pip install -e ".[dev]"`

### Basic Usage

**Option 1: Using the command-line tool**
```bash
# Interactive mode (prompts for import)
daw-run

# Skip data import, process existing files
daw-run --skip-import

# Import fresh data and process
daw-run --import

# Verbose output with detailed progress
daw-run --import --verbose
```

**Option 2: Using Python module execution**
```bash
python -m daw_nyc --skip-import
```

**Option 3: As a Python library**
```python
from daw_nyc.libs.daw import NYCDataPipeline

# Create pipeline instance
pipeline = NYCDataPipeline()

# Load data
df_311, df_rent, uhf_mapping, manual_mapping = pipeline.load_data()

# Clean data
df_311_clean, df_rent_clean = pipeline.clean_data(df_311, df_rent)

# Transform data
df_311_transformed, df_rent_transformed = pipeline.transform_data(
    df_311_clean, df_rent_clean, uhf_mapping, manual_mapping
)

# Create final integrated dataset
final_df = pipeline.aggregate_and_integrate(
    df_311_transformed, df_rent_transformed
)
```

---

## 📖 Documentation

### Project Structure

```
RAW_NYC_data/
├── daw_nyc/                    # Main package
│   ├── __init__.py
│   ├── __main__.py             # CLI entry point
│   ├── config/                 # Configuration management
│   │   ├── __init__.py
│   │   ├── config.py           # Settings with defaults
│   │   └── utils.py            # Config utilities
│   └── libs/                   # Core libraries
│       ├── daw/                # Data wrangling modules
│       │   ├── __init__.py
│       │   ├── pipeline.py     # Main pipeline orchestrator
│       │   ├── data_loader.py  # Data loading utilities
│       │   ├── data_cleaner.py # Data cleaning operations
│       │   ├── data_transformer.py  # Data transformations
│       │   └── data_integrator.py   # Data integration
│       ├── import_data/        # Data import utilities
│       │   ├── __init__.py
│       │   ├── fetcher.py      # NYC 311 API fetcher
│       │   ├── rent_downloader.py  # Rent data downloader
│       │   └── utils.py        # Helper functions
│       └── test_setup/         # Test data utilities
├── tests/                      # Test suite
│   ├── conftest.py
│   ├── test_pipeline.py
│   ├── test_data_loader.py
│   ├── test_data_cleaner.py
│   ├── test_data_transformer.py
│   └── test_data_integrator.py
├── data/   
│   └── sample-test.parquet                   # CSV Data datafiles (gitignored)
├── mappings/                   # Neighborhood mappings
│   ├── nyc_uhf_zipcodes.json
│   └── manual_map.json
├── notebooks/                  # Jupyter notebooks
├── gdv/                        # Submodules for gdv module
├── pyproject.toml              # Package configuration
└── README.md                   # This file
```

### Architecture

The project follows a **modular pipeline architecture**:

```
┌─────────────────────────────────────────────────────────────┐
│                     NYC Data Pipeline                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ Data Import  │───▶│ Data Wrangling│───▶│   Output     │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                              │
│  • NYC 311 API       • Load              • Integrated CSV   │
│  • StreetEasy        • Clean             • Ready for GDV    │
│                      • Transform                            │
│                      • Integrate                            │
└─────────────────────────────────────────────────────────────┘
```

**Data Flow:**

1. **Import Pipeline** (`libs/import_data/`)
   - Fetches NYC 311 data via Socrata API
   - Downloads median rent data from StreetEasy
   - Stratified sampling for representative datasets

2. **Data Wrangling Pipeline** (`libs/daw/`)
   - **Load**: Read CSV files and JSON mappings
   - **Clean**: Handle missing values, duplicates, invalid data
   - **Transform**: Create features, map neighborhoods, aggregate
   - **Integrate**: Merge datasets, validate, optimize

3. **Output**
   - `data/data_snapshot_for_gdv.csv` - Final integrated dataset

### Core Modules

#### `daw_nyc.libs.daw.pipeline`
Main orchestrator for the data processing pipeline.

**Key Classes:**
- `NYCDataPipeline` - Step-by-step pipeline execution

**Methods:**
- `load_data()` - Load all required datasets
- `clean_data()` - Clean and preprocess data
- `transform_data()` - Apply transformations and create features
- `aggregate_and_integrate()` - Merge and finalize dataset

#### `daw_nyc.libs.daw.data_loader`
Handles loading of datasets and mappings.

**Key Classes:**
- `DataLoader` - Loads CSV files, JSON mappings

#### `daw_nyc.libs.daw.data_cleaner`
Data cleaning and preprocessing operations.

**Key Classes:**
- `DataCleaner` - Cleans NYC 311 and rent data

**Operations:**
- Remove duplicates
- Handle missing values
- Clean date columns
- Validate geographic data

#### `daw_nyc.libs.daw.data_transformer`
Data transformation and feature engineering.

**Key Classes:**
- `DataTransformer` - Creates features, maps neighborhoods

**Operations:**
- Time feature extraction (year, month, day_of_week)
- Resolution time calculation
- Neighborhood mapping (ZIP → UHF neighborhood)
- Borough mapping
- Aggregations

#### `daw_nyc.libs.daw.data_integrator`
Data integration and final dataset creation.

**Key Classes:**
- `DataIntegrator` - Merges datasets, validates quality

**Operations:**
- Merge complaints and rent data
- Data quality validation
- Type optimization for memory efficiency
- Export to CSV

---

## ⚙️ Configuration

All configuration is managed in `daw_nyc/config/config.py` with sensible defaults.

### Key Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `BASE_URL` | NYC Open Data API | NYC 311 data source |
| `DEFAULT_SINCE` | 2024 | Start year for data collection |
| `DEFAULT_UNTIL` | 2025 | End year for data collection |
| `TARGET_SAMPLE` | 10000 | Target sample size per month |
| `BASE_DATA_PATH` | `../../data` | Data directory path |
| `TIMEOUT` | 60 | API request timeout (seconds) |
| `MAX_RETRIES` | 5 | Maximum API retry attempts |

### Environment Variables (Optional)

You can override defaults using environment variables:

```bash
export BASE_URL="https://custom-api-url.com"
export TARGET_SAMPLE=5000
export BASE_DATA_PATH="/custom/path/to/data"

daw-run
```

### Configuration File

Default constants are defined in `daw_nyc/config/config.py`:

```python
from daw_nyc.config import get_settings

settings = get_settings()
print(settings.TARGET_SAMPLE)  # 10000
print(settings.BASE_DATA_PATH)  # ../../data
```

---

## 🎯 Command-Line Interface

The `daw-run` command provides a flexible CLI for running the data pipeline.

### CLI Options

#### Pipeline Control

```bash
# Interactive mode - prompts whether to import data
daw-run

# Run import pipeline + data processing
daw-run --import

# Skip import, use existing data files
daw-run --skip-import

# Only import data, skip processing
daw-run --import-only
```

#### Data Configuration

```bash
# Override target sample size per month
daw-run --import --target-sample 5000

# Custom date range
daw-run --import --since 2023 --until 2024

# Combine multiple options
daw-run --import --target-sample 3000 --since 2024 --until 2025
```

#### Behavior Options

```bash
# Verbose output with emoji-enhanced progress
daw-run --import --verbose
daw-run --import -v

# Quiet mode - minimal output
daw-run --skip-import --quiet
daw-run --skip-import -q

# Show version
daw-run --version

# Show help
daw-run --help
```

#### Complete Reference

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--import` | - | flag | Run import pipeline to fetch fresh data |
| `--skip-import` | - | flag | Skip import, use existing data files |
| `--import-only` | - | flag | Only import, skip data processing |
| `--target-sample` | - | int | Override target sample size per month |
| `--since` | - | int | Start year for data collection |
| `--until` | - | int | End year for data collection |
| `--verbose` | `-v` | flag | Enable verbose output |
| `--quiet` | `-q` | flag | Suppress non-essential output |
| `--version` | - | flag | Show version and exit |
| `--help` | `-h` | flag | Show help message and exit |

### Output

The pipeline always outputs to:
```
data/data_snapshot_for_gdv.csv
```

This file contains the final integrated dataset ready for analysis and visualization.

---

## 💻 Usage Examples

> **📚 For comprehensive examples, see [EXAMPLES.md](EXAMPLES.md)**

### Example 1: Run Complete Pipeline

```bash
# Interactive mode - you'll be prompted
daw-run
# Prompt: "Do you want to refresh source data? [y/n]:"
# YES (y): Downloads fresh data from APIs, then processes it
# NO (n):  Uses existing CSV files and processes them

# Non-interactive: import fresh data
daw-run --import

# Non-interactive: use existing data
daw-run --skip-import

# Import only, no processing
daw-run --import-only

# Verbose mode with detailed progress
daw-run --import --verbose
```

### Example 2: Custom Data Collection

```bash
# Collect smaller dataset (3000 records per month)
daw-run --import --target-sample 3000

# Collect data for specific years
daw-run --import --since 2023 --until 2024

# Combine: custom sample size and date range
daw-run --import --target-sample 5000 --since 2024 --until 2025 --verbose
```

### Example 3: Import Data Programmatically

```python
from daw_nyc.libs.import_data import download_rent_data_if_missing
from daw_nyc.libs.import_data import get_dataset_stratified
from daw_nyc.config import get_settings

# Download rent data
download_rent_data_if_missing(output_dir="data")

# Fetch NYC 311 data
settings = get_settings()
df_311 = get_dataset_stratified(months, settings, settings.SELECTED_COLUMNS)
```

### Example 4: Use Pipeline in Jupyter Notebook

```python
# In a Jupyter notebook
from daw_nyc.libs.daw import NYCDataPipeline
import pandas as pd

# Initialize pipeline
pipeline = NYCDataPipeline(base_path=".")

# Load data
df_311, df_rent, uhf, manual = pipeline.load_data()

# Process step by step
df_311_clean, df_rent_clean = pipeline.clean_data(df_311, df_rent)
df_311_trans, df_rent_trans = pipeline.transform_data(
    df_311_clean, df_rent_clean, uhf, manual
)

# Create final dataset
final = pipeline.aggregate_and_integrate(df_311_trans, df_rent_trans)

# Analyze
print(final.shape)
print(final.columns)
final.head()
```

### Example 5: Custom Data Processing

```python
from daw_nyc.libs.daw import DataCleaner, DataTransformer
import pandas as pd

# Load your own data
df = pd.read_csv("my_data.csv")

# Use individual components
cleaner = DataCleaner()
df_clean = cleaner.clean_nyc_311_data(df, relevant_columns=['date', 'borough'])

transformer = DataTransformer()
df_with_features = transformer.create_time_features(df_clean)
```

---

## 🧪 Testing

The project includes a comprehensive test suite with 95%+ code coverage.

### Run Tests

```bash
# Run all tests
pytest tests/

# Run with coverage report
pytest tests/ --cov=daw_nyc --cov-report=html

# View coverage report
open htmlcov/index.html  # macOS
```

### Test Structure

```
tests/
├── conftest.py                 # Shared fixtures
├── test_pipeline.py            # Pipeline integration tests
├── test_data_loader.py         # Loader unit tests
├── test_data_cleaner.py        # Cleaner unit tests
├── test_data_transformer.py    # Transformer unit tests
└── test_data_integrator.py     # Integrator unit tests
```

---

## 📁 Data Sources

### NYC 311 Service Requests
- **Source**: [NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data)
- **Dataset**: 311 Service Requests
- **API**: Socrata SODA API
- **Fields**: Complaint type, location, dates, resolution status

### Median Rent Data
- **Source**: [StreetEasy](https://streeteasy.com/blog/data-dashboard/?utm_source=chatgpt.com)
- **Dataset**: Median Asking Rent - All
- **Format**: ZIP archive containing CSV
- **Granularity**: Monthly, by neighborhood

### Geographic Mappings
- **UHF Neighborhoods**: NYC Health Department UHF42 boundaries
- **ZIP to Neighborhood**: Custom mapping from ZIP codes to UHF areas
- **Manual Mappings**: Hand-curated area name corrections

---

## 🛠️ Development

### Setup Development Environment

```bash
# Install with development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks (optional)
pip install pre-commit
pre-commit install
```

### Running Linters

```bash
# Format code with black
black daw_nyc/

# Run type checker
mypy daw_nyc/

# Run linter
pylint daw_nyc/
```

### Building Distribution

```bash
# Build package
python -m build

# Install built package
pip install dist/daw_nyc-0.1.0-py3-none-any.whl
```

---

## 🤝 Contributing

### Team Members
- Roberto Fazekas (roberto.fazekas@students.fhnw.ch)
- Mykhailo Andrusiak (mykhailo.andrusiak@students.fhnw.ch)

---

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 📚 Additional Resources

- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- [NYC 311 API Documentation](https://dev.socrata.com/)
- [StreetEasy Research](https://streeteasy.com/blog/data-dashboard/)
- [Python Packaging Guide](https://packaging.python.org/)

---

**Last Updated**: November 2025