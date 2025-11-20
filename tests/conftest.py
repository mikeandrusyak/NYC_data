import pytest
import pandas as pd
import json
from pathlib import Path
from unittest.mock import mock_open, patch
from typing import Dict, Any


# Helper functions for creating test data
def create_sample_311_dataframe():
    """Create a sample NYC 311 DataFrame for testing."""
    return pd.DataFrame({
        'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking', 'Street Light Condition', 'Water System'],
        'created_date': ['2024-01-01 10:30:00', '2024-01-02 14:15:00', '2024-01-03 09:45:00', '2024-01-04 16:20:00', '2024-01-05 11:00:00'],
        'closed_date': ['2024-01-02 12:00:00', None, '2024-01-03 15:30:00', '2024-01-05 08:00:00', None],
        'incident_zip': ['10001', '10002', '10003', '10004', '10005'],
        'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND'],
        'latitude': [40.7128, 40.6782, 40.7282, 40.8448, 40.5795],
        'longitude': [-74.0060, -73.9442, -73.7949, -73.8648, -74.1502],
        'city': ['NEW YORK', 'NEW YORK', 'NEW YORK', 'NEW YORK', 'NEW YORK'],
        'status': ['Closed', 'Open', 'Closed', 'Closed', 'Open'],
        'agency': ['NYPD', 'HPD', 'NYPD', 'DOT', 'DEP']
    }, index=pd.Index([1001, 1002, 1003, 1004, 1005], name='unique_key'))


def create_sample_rent_dataframe():
    """Create a sample rent DataFrame for testing."""
    return pd.DataFrame({
        'areaname': ['East Village', 'Williamsburg', 'Astoria', 'Park Slope', 'Long Island City'],
        'areaid': [1, 2, 3, 4, 5],
        'year': [2024, 2024, 2024, 2024, 2024],
        'rent_2024': [3000, 2800, 2400, 3200, 2600],
        'rent_2023': [2900, 2700, 2300, 3100, 2500],
        'rent_2022': [2800, 2600, 2200, 3000, 2400]
    })


def create_sample_uhf_mapping():
    """Create a sample UHF mapping for testing."""
    return {
        "data": [
            {"uhf_neigh": "East Village", "zipcode": "10001"},
            {"uhf_neigh": "Williamsburg", "zipcode": "10002"},
            {"uhf_neigh": "Astoria", "zipcode": "10003"},
            {"uhf_neigh": "Park Slope", "zipcode": "10004"},
            {"uhf_neigh": "Long Island City", "zipcode": "10005"}
        ]
    }


def create_sample_manual_mapping():
    """Create a sample manual mapping for testing."""
    return {
        "East Village": "East Village - LES",
        "Williamsburg": "Williamsburg - North Brooklyn",
        "Astoria": "Astoria - Queens",
        "Park Slope": "Park Slope - Brooklyn",
        "Long Island City": "Long Island City - Queens"
    }


def create_dirty_311_dataframe():
    """Create a 311 DataFrame with data quality issues for testing DataCleaner."""
    return pd.DataFrame({
        'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking', 'Heat/Hot Water', 'Water System'],
        'created_date': ['2024-01-01 10:30:00', '2024-01-02 14:15:00', '2024-01-05 09:45:00', '2024-01-02 14:15:00', 'invalid_date'],
        'closed_date': ['2024-01-02 12:00:00', None, '2024-01-03 15:30:00', None, '2024-01-01 08:00:00'],  # Last row has closed < created
        'resolution_action_updated_date': ['2024-01-02 11:00:00', None, None, None, None],
        'incident_zip': ['10001', None, '10003', '10002', '10005'],
        'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BROOKLYN', 'STATEN ISLAND'],
        'latitude': [40.7128, 40.6782, 40.7282, 40.6782, 40.5795],
        'longitude': [-74.0060, -73.9442, -73.7949, -73.9442, -74.1502],
        'city': ['NEW YORK  ', ' new york', 'FLORAL PARK', '  NEW YORK', 'BREEZY POINT'],  # Mixed case, whitespace, outside NYC
        'status': ['Closed', 'Open', 'Closed', 'Open', 'Open'],
        'agency': ['NYPD', 'HPD', 'NYPD', 'HPD', 'DEP']
    })  # Create duplicate rows by repeating row 1


def create_dirty_rent_dataframe():
    """Create a rent DataFrame with data quality issues for testing DataCleaner."""
    return pd.DataFrame({
        'areaname': ['East Village', 'Williamsburg', 'Astoria', 'East Village', 'Long Island City'],  # Duplicate
        'areaid': [1, 2, 3, 1, 5],  # Duplicate
        'year': [2024, 2024, 2024, 2024, 2024],  # Duplicate
        '2024-01': [3000, 2800, None, 3000, 2600],  # Missing value
        '2024-02': [3050, 2850, 2450, 3050, 2650],
        '2025-01': [3100, 2900, 2500, 3100, 2700],
        '2023-12': [2950, 2750, 2350, 2950, 2550],
        '2022-12': [2800, 2600, 2200, 2800, 2400]
    })


def create_dataframe_with_duplicates():
    """Create a DataFrame specifically designed to have duplicate rows."""
    df = pd.DataFrame({
        'complaint_type': ['Noise - Residential', 'Heat/Hot Water'],
        'created_date': ['2024-01-01 10:30:00', '2024-01-02 14:15:00'],
        'borough': ['MANHATTAN', 'BROOKLYN']
    })
    # Add duplicate of first row to create actual duplicates
    return pd.concat([df, df.iloc[[0]]], ignore_index=True)


def create_aggregated_complaints_data():
    """Create aggregated complaints data for integration testing."""
    return pd.DataFrame({
        'neighborhood': ['East Village', 'Williamsburg', 'Astoria', 'Park Slope', 'Long Island City'],
        'year': [2024, 2024, 2024, 2024, 2024],
        'month': [1, 1, 1, 1, 1],
        'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking', 'Street Condition', 'Water System'],
        'complaint_count': [150, 89, 234, 45, 78],
        'median_resolution_time_hours': [24.5, 72.0, 12.5, 48.0, 36.0],
        'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BROOKLYN', 'QUEENS']
    })


def create_reshaped_rent_data():
    """Create reshaped rent data for integration testing."""
    return pd.DataFrame({
        'neighborhood': ['East Village', 'Williamsburg', 'Astoria', 'Park Slope', 'Long Island City', 'SoHo'],
        'year': [2024, 2024, 2024, 2024, 2024, 2024],
        'month': [1, 1, 1, 1, 1, 1],
        'median_rent': [3000.0, 2800.0, 2400.0, 3200.0, 2600.0, 4500.0]
    })


def create_final_integrated_dataset():
    """Create a final integrated dataset for testing."""
    return pd.DataFrame({
        'neighborhood': ['East Village', 'Williamsburg', 'Astoria', 'Park Slope'],
        'year': [2024, 2024, 2024, 2024],
        'month': [1, 1, 1, 1],
        'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking', 'Street Condition'],
        'complaint_count': [150, 89, 234, 45],
        'median_resolution_time_hours': [24.5, 72.0, 12.5, 48.0],
        'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BROOKLYN'],
        'median_rent': [3000.0, 2800.0, 2400.0, 3200.0]
    })


def create_dataset_with_data_quality_issues():
    """Create a dataset with various data quality issues for validation testing."""
    return pd.DataFrame({
        'neighborhood': ['East Village', None, 'Astoria', 'Park Slope', 'Duplicate'],
        'year': [2024, 2024, 2023, 2026, 2024],  # Invalid years
        'month': [1, 0, 13, 1, 1],  # Invalid months
        'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking', 'Street Condition', 'Noise - Residential'],
        'complaint_count': [150, -5, 234, 45, 150],  # Negative count
        'median_resolution_time_hours': [24.5, -10.0, 12.5, 48.0, 24.5],  # Negative resolution time
        'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BROOKLYN', 'MANHATTAN'],
        'median_rent': [3000.0, None, 2400.0, 3200.0, 3000.0]  # Missing rent data
    })


def create_dataset_for_optimization():
    """Create a dataset suitable for data type optimization testing."""
    n_rows = 200
    
    return pd.DataFrame({
        'neighborhood': (['East Village'] * 100 + ['Williamsburg'] * 100),  # High repetition - categorical
        'complaint_type': (['Noise'] * 150 + ['Heat'] * 50),  # High repetition - categorical  
        'year': [2024] * n_rows,  # Small integer range
        'month': (list(range(1, 13)) * 17)[:n_rows],  # Small integer range, repeating pattern
        'complaint_count': list(range(1, n_rows + 1)),  # Integer values 1-200
        'median_rent': [float(x * 10.5) for x in range(1, n_rows + 1)],  # Float values
        'resolution_hours': [x / 2.0 for x in range(1, n_rows + 1)]  # Float values
    })


def create_pipeline_test_data():
    """Create comprehensive test data for pipeline testing."""
    return {
        'raw_nyc_311': pd.DataFrame({
            'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking'],
            'created_date': ['2024-01-01 10:30:00', '2024-01-02 14:15:00', '2024-01-03 09:45:00'],
            'closed_date': ['2024-01-02 12:00:00', None, '2024-01-03 15:30:00'],
            'incident_zip': ['10001', '10002', '10003'],
            'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS'],
            'latitude': [40.7128, 40.6782, 40.7282],
            'longitude': [-74.0060, -73.9442, -73.7949],
            'city': ['NEW YORK', 'NEW YORK', 'NEW YORK'],
            'status': ['Closed', 'Open', 'Closed'],
            'agency': ['NYPD', 'HPD', 'NYPD']
        }),
        'raw_rent': pd.DataFrame({
            'areaname': ['East Village', 'Williamsburg', 'Astoria'],
            'areaid': [1, 2, 3],
            'year': [2024, 2024, 2024],
            '2024-01': [3000, 2800, 2400],
            '2024-02': [3050, 2850, 2450]
        }),
        'uhf_mapping': {
            "data": [
                {"uhf_neigh": "East Village", "zipcode": "10001"},
                {"uhf_neigh": "Williamsburg", "zipcode": "10002"},
                {"uhf_neigh": "Astoria", "zipcode": "10003"}
            ]
        },
        'manual_mapping': {
            "East Village": "East Village - LES",
            "Williamsburg": "Williamsburg - North Brooklyn",
            "Astoria": "Astoria - Queens"
        }
    }


def create_pipeline_intermediate_data():
    """Create intermediate data states for pipeline testing."""
    return {
        'cleaned_nyc_311': pd.DataFrame({
            'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking'],
            'created_date': pd.to_datetime(['2024-01-01 10:30:00', '2024-01-02 14:15:00', '2024-01-03 09:45:00']),
            'closed_date': pd.to_datetime(['2024-01-02 12:00:00', None, '2024-01-03 15:30:00']),
            'incident_zip': ['10001', '10002', '10003'],
            'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS'],
            'city': ['NEW YORK', 'NEW YORK', 'NEW YORK']
        }),
        'cleaned_rent': pd.DataFrame({
            'areaname': ['East Village', 'Williamsburg', 'Astoria'],
            'areaid': [1, 2, 3],
            'year': [2024, 2024, 2024],
            '2024-01': [3000, 2800, 2400],
            '2024-02': [3050, 2850, 2450]
        }),
        'transformed_nyc_311': pd.DataFrame({
            'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking'],
            'neighborhood': ['East Village', 'Williamsburg', 'Astoria'],
            'year': [2024, 2024, 2024],
            'month': [1, 1, 1],
            'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS'],
            'created_date': pd.to_datetime(['2024-01-01 10:30:00', '2024-01-02 14:15:00', '2024-01-03 09:45:00']),
            'resolution_time_hours': [25.5, None, 29.75]
        }),
        'transformed_rent': pd.DataFrame({
            'neighborhood': ['East Village - LES', 'Williamsburg - North Brooklyn', 'Astoria - Queens'],
            'year': [2024, 2024, 2024],
            'month': [1, 1, 1],
            'median_rent': [3000.0, 2800.0, 2400.0]
        })
    }


def create_datetime_311_dataframe():
    """Create a 311 DataFrame with datetime columns for transformer testing."""
    return pd.DataFrame({
        'complaint_type': ['Noise - Residential', 'Heat/Hot Water', 'Illegal Parking'],
        'created_date': pd.to_datetime(['2024-01-01 10:30:00', '2024-02-15 14:15:00', '2024-03-20 09:45:00']),
        'closed_date': pd.to_datetime(['2024-01-02 12:00:00', '2024-02-17 16:30:00', None]),
        'incident_zip': ['10001', '10002', '10003'],
        'borough': ['MANHATTAN', 'BROOKLYN', 'QUEENS'],
        'status': ['Closed', 'Closed', 'Open']
    })


def create_sample_uhf_data():
    """Create sample UHF data structure for testing."""
    return {
        "Manhattan": [
            {
                "neighborhood": "East Village",
                "zip_codes": ["10001", "10003", "10009"]
            },
            {
                "neighborhood": "Upper West Side",
                "zip_codes": ["10024", "10025"]
            }
        ],
        "Brooklyn": [
            {
                "neighborhood": "Williamsburg", 
                "zip_codes": ["11211", "11206"]
            },
            {
                "neighborhood": "Park Slope",
                "zip_codes": ["11215", "11217"]
            }
        ]
    }


def create_rent_data_for_transform():
    """Create rent DataFrame for transformation testing."""
    return pd.DataFrame({
        'areaname': ['East Village', 'Williamsburg', 'Park Slope', 'Upper West Side'],
        'areaid': [1, 2, 3, 4],
        'year': [2024, 2024, 2024, 2024],
        '2024-01': [3000, 2800, 3200, 3500],
        '2024-02': [3050, 2850, 3250, 3550],
        '2024-03': [3100, 2900, 3300, 3600],
        'neighborhood': [None, None, None, None]  # Will be filled by transformation
    })


# Fixtures for mock data
@pytest.fixture
def sample_nyc_311_data():
    """Fixture providing sample NYC 311 data."""
    return create_sample_311_dataframe()


@pytest.fixture
def sample_rent_data():
    """Fixture providing sample rent data."""
    return create_sample_rent_dataframe()


@pytest.fixture
def sample_uhf_mapping():
    """Fixture providing sample UHF mapping."""
    return create_sample_uhf_mapping()


@pytest.fixture
def sample_manual_mapping():
    """Fixture providing sample manual mapping."""
    return create_sample_manual_mapping()


@pytest.fixture
def dirty_nyc_311_data():
    """Fixture providing dirty NYC 311 data for testing DataCleaner."""
    return create_dirty_311_dataframe()


@pytest.fixture
def dirty_rent_data():
    """Fixture providing dirty rent data for testing DataCleaner."""
    return create_dirty_rent_dataframe()


@pytest.fixture
def dataframe_with_duplicates():
    """Fixture providing DataFrame with actual duplicate rows."""
    return create_dataframe_with_duplicates()


@pytest.fixture
def aggregated_complaints_data():
    """Fixture providing aggregated complaints data for integration testing."""
    return create_aggregated_complaints_data()


@pytest.fixture
def reshaped_rent_data():
    """Fixture providing reshaped rent data for integration testing."""
    return create_reshaped_rent_data()


@pytest.fixture
def final_integrated_dataset():
    """Fixture providing final integrated dataset for testing."""
    return create_final_integrated_dataset()


@pytest.fixture
def dataset_with_data_quality_issues():
    """Fixture providing dataset with data quality issues for validation testing."""
    return create_dataset_with_data_quality_issues()


@pytest.fixture
def dataset_for_optimization():
    """Fixture providing dataset suitable for optimization testing."""
    return create_dataset_for_optimization()


@pytest.fixture
def pipeline_test_data():
    """Fixture providing comprehensive test data for pipeline testing."""
    return create_pipeline_test_data()


@pytest.fixture
def pipeline_intermediate_data():
    """Fixture providing intermediate data states for pipeline testing."""
    return create_pipeline_intermediate_data()


@pytest.fixture
def datetime_311_data():
    """Fixture providing 311 data with datetime columns."""
    return create_datetime_311_dataframe()


@pytest.fixture
def sample_uhf_data():
    """Fixture providing sample UHF data structure."""
    return create_sample_uhf_data()


@pytest.fixture
def rent_data_for_transform():
    """Fixture providing rent data for transformation testing."""
    return create_rent_data_for_transform()


# Legacy fixture for backward compatibility
@pytest.fixture
def df():
    # Pfad zum Sample aus Git (klein & committed!)
    project_root = Path(__file__).resolve().parents[1]
    sample_path = project_root / "data" / "sample-test.parquet"

    if not sample_path.exists():
        raise FileNotFoundError(
            f"Sample file not found: {sample_path}. "
            "Generate it with create_test_sample() first."
        )

    df = pd.read_parquet(sample_path)

    # Sicherstellen, dass created_date richtig ist:
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    return df