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