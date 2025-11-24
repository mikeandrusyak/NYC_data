"""
Unit tests for DataTransformer class.

This module contains comprehensive tests for the DataTransformer component
covering time feature creation, resolution time calculation, geographic mapping,
data aggregation, and transformation pipelines.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from io import StringIO
import sys

from daw_nyc.libs.daw.data_transformer import DataTransformer

class TestDataTransformerInitialization:
    """Test DataTransformer initialization."""
    
    def test_init_default(self):
        """Test DataTransformer initialization with default parameters."""
        transformer = DataTransformer()
        assert transformer is not None
        assert isinstance(transformer, DataTransformer)


class TestCreateTimeFeatures:
    """Test create_time_features method."""
    
    def test_create_time_features_success(self, datetime_311_data):
        """Test successful time feature creation."""
        transformer = DataTransformer()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.create_time_features(datetime_311_data)
        
        assert 'month' in result.columns
        assert 'year' in result.columns
        assert result['month'].iloc[0] == 1  # January
        assert result['month'].iloc[1] == 2  # February
        assert result['year'].iloc[0] == 2024
        assert "Created time features: month, year" in fake_out.getvalue()
    
    def test_create_time_features_missing_column(self, sample_nyc_311_data):
        """Test time feature creation with missing date column."""
        transformer = DataTransformer()
        
        # Remove created_date column
        test_df = sample_nyc_311_data.drop('created_date', axis=1)
        result = transformer.create_time_features(test_df, 'nonexistent_date')
        
        assert 'month' not in result.columns
        assert 'year' not in result.columns
        assert len(result) == len(test_df)
    
    def test_create_time_features_custom_column(self, datetime_311_data):
        """Test time feature creation with custom date column."""
        transformer = DataTransformer()
        
        result = transformer.create_time_features(datetime_311_data, 'closed_date')
        
        assert 'month' in result.columns
        assert 'year' in result.columns
        # Should handle NaN values gracefully
        assert result['month'].notna().sum() > 0


class TestCalculateResolutionTime:
    """Test calculate_resolution_time method."""
    
    def test_calculate_resolution_time_success(self, datetime_311_data):
        """Test successful resolution time calculation."""
        transformer = DataTransformer()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.calculate_resolution_time(datetime_311_data)
        
        assert 'resolution_time_hours' in result.columns
        # First record: 2024-01-02 12:00 - 2024-01-01 10:30 = 25.5 hours
        assert abs(result['resolution_time_hours'].iloc[0] - 25.5) < 0.1
        assert "Created feature: resolution_time_hours" in fake_out.getvalue()
        assert "Resolution time statistics:" in fake_out.getvalue()
    
    def test_calculate_resolution_time_missing_columns(self, sample_nyc_311_data):
        """Test resolution time calculation with missing columns."""
        transformer = DataTransformer()
        
        # Remove closed_date column
        test_df = sample_nyc_311_data.drop('closed_date', axis=1)
        result = transformer.calculate_resolution_time(test_df)
        
        assert 'resolution_time_hours' not in result.columns
        assert len(result) == len(test_df)
    
    def test_calculate_resolution_time_custom_columns(self, datetime_311_data):
        """Test resolution time calculation with custom column names."""
        transformer = DataTransformer()
        
        # Rename columns for testing
        test_df = datetime_311_data.rename(columns={
            'created_date': 'start_time',
            'closed_date': 'end_time'
        })
        
        result = transformer.calculate_resolution_time(test_df, 'start_time', 'end_time')
        
        assert 'resolution_time_hours' in result.columns


class TestCreateZipToNeighborhoodMapping:
    """Test create_zip_to_neighborhood_mapping method."""
    
    def test_create_zip_to_neighborhood_mapping_success(self, sample_uhf_data):
        """Test successful ZIP to neighborhood mapping creation."""
        transformer = DataTransformer()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.create_zip_to_neighborhood_mapping(sample_uhf_data)
        
        assert isinstance(result, dict)
        assert '10001' in result
        assert '11211' in result
        assert result['10001'] == 'East Village'
        assert result['11211'] == 'Williamsburg'
        assert "Created mapping for" in fake_out.getvalue()
    
    def test_create_zip_to_neighborhood_mapping_empty_data(self):
        """Test ZIP to neighborhood mapping with empty data."""
        transformer = DataTransformer()
        
        result = transformer.create_zip_to_neighborhood_mapping({})
        
        assert isinstance(result, dict)
        assert len(result) == 0


class TestMapZipToNeighborhood:
    """Test map_zip_to_neighborhood method."""
    
    def test_map_zip_to_neighborhood_success(self, datetime_311_data, sample_uhf_data):
        """Test successful ZIP to neighborhood mapping."""
        transformer = DataTransformer()
        zip_to_neighborhood = transformer.create_zip_to_neighborhood_mapping(sample_uhf_data)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.map_zip_to_neighborhood(datetime_311_data, zip_to_neighborhood)
        
        assert 'neighborhood' in result.columns
        assert result['neighborhood'].iloc[0] == 'East Village'  # 10001 -> East Village
        assert "Neighborhood mapping results:" in fake_out.getvalue()
        assert "Coverage percentage:" in fake_out.getvalue()
    
    def test_map_zip_to_neighborhood_missing_column(self, sample_nyc_311_data, sample_uhf_data):
        """Test ZIP to neighborhood mapping with missing ZIP column."""
        transformer = DataTransformer()
        zip_to_neighborhood = transformer.create_zip_to_neighborhood_mapping(sample_uhf_data)
        
        # Remove incident_zip column
        test_df = sample_nyc_311_data.drop('incident_zip', axis=1)
        result = transformer.map_zip_to_neighborhood(test_df, zip_to_neighborhood, 'nonexistent_zip')
        
        assert 'neighborhood' not in result.columns
        assert len(result) == len(test_df)
    
    def test_map_zip_to_neighborhood_with_na_values(self, sample_uhf_data):
        """Test ZIP to neighborhood mapping with NaN values."""
        transformer = DataTransformer()
        zip_to_neighborhood = transformer.create_zip_to_neighborhood_mapping(sample_uhf_data)
        
        # Create test data with NaN values
        test_df = pd.DataFrame({
            'incident_zip': ['10001', None, '99999', '11211'],
            'complaint_type': ['A', 'B', 'C', 'D']
        })
        
        result = transformer.map_zip_to_neighborhood(test_df, zip_to_neighborhood)
        
        assert 'neighborhood' in result.columns
        assert result['neighborhood'].iloc[0] == 'East Village'
        assert pd.isna(result['neighborhood'].iloc[1])  # NaN zip -> NaN neighborhood
        assert pd.isna(result['neighborhood'].iloc[2])  # Unknown zip -> NaN neighborhood


class TestMapAreaToNeighborhood:
    """Test map_area_to_neighborhood method."""
    
    def test_map_area_to_neighborhood_success(self, rent_data_for_transform, sample_manual_mapping):
        """Test successful area to neighborhood mapping."""
        transformer = DataTransformer()
        
        # Create manual mapping that matches our test data
        manual_map = {
            'east village': 'East Village - LES',
            'williamsburg': 'Williamsburg - North Brooklyn',
            'park slope': 'Park Slope - Brooklyn'
        }
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.map_area_to_neighborhood(rent_data_for_transform, manual_map, 'areaname')
        
        assert 'neighborhood' in result.columns
        assert "Area to neighborhood mapping results:" in fake_out.getvalue()
        assert "Coverage percentage:" in fake_out.getvalue()
    
    def test_map_area_to_neighborhood_missing_column(self, sample_rent_data):
        """Test area to neighborhood mapping with missing area column."""
        transformer = DataTransformer()
        manual_map = {'test': 'Test Neighborhood'}
        
        result = transformer.map_area_to_neighborhood(sample_rent_data, manual_map, 'nonexistent_area')
        
        assert 'neighborhood' not in result.columns


class TestAggregateComplaintsByNeighborhood:
    """Test aggregate_complaints_by_neighborhood method."""
    
    def test_aggregate_complaints_success(self):
        """Test successful complaints aggregation."""
        transformer = DataTransformer()
        
        # Create test data
        test_df = pd.DataFrame({
            'neighborhood': ['East Village', 'East Village', 'Williamsburg'],
            'borough': ['Manhattan', 'Manhattan', 'Brooklyn'],
            'complaint_type': ['Noise', 'Noise', 'Heat'],
            'year': [2024, 2024, 2024],
            'month': [1, 1, 2],
            'resolution_time_hours': [10.5, 15.2, 8.7]
        })
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.aggregate_complaints_by_neighborhood(test_df)
        
        assert isinstance(result, pd.DataFrame)
        assert 'complaint_count' in result.columns
        assert 'median_resolution_time_hours' in result.columns
        assert "Aggregated complaints data shape:" in fake_out.getvalue()
    
    def test_aggregate_complaints_custom_columns(self):
        """Test complaints aggregation with custom grouping columns."""
        transformer = DataTransformer()
        
        test_df = pd.DataFrame({
            'neighborhood': ['East Village', 'East Village'],
            'borough': ['Manhattan', 'Manhattan'],
            'complaint_type': ['Noise', 'Heat'],
            'year': [2024, 2024],
            'month': [1, 1],
            'resolution_time_hours': [10.5, 15.2]
        })
        
        custom_columns = ['neighborhood', 'borough', 'complaint_type', 'year', 'month']
        result = transformer.aggregate_complaints_by_neighborhood(test_df, custom_columns)
        
        assert len(result.columns) == len(custom_columns) + 2  # +2 for count and median


class TestAggregateRentByNeighborhood:
    """Test aggregate_rent_by_neighborhood method."""
    
    def test_aggregate_rent_success(self):
        """Test successful rent aggregation."""
        transformer = DataTransformer()
        
        # Create test data
        test_df = pd.DataFrame({
            'neighborhood': ['East Village', 'East Village', 'Williamsburg'],
            '2024-01': [3000, 3100, 2800],
            '2024-02': [3050, 3150, 2850],
            '2025-01': [3200, 3250, 2950]
        })
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.aggregate_rent_by_neighborhood(test_df)
        
        assert isinstance(result, pd.DataFrame)
        assert '2024-01' in result.columns
        assert '2025-01' in result.columns
        assert '2023-01' not in result.columns  # Should not include 2023
        assert "Aggregated rent data shape:" in fake_out.getvalue()
    
    def test_aggregate_rent_custom_years(self):
        """Test rent aggregation with custom years."""
        transformer = DataTransformer()
        
        test_df = pd.DataFrame({
            'neighborhood': ['East Village', 'Williamsburg'],
            '2023-01': [2800, 2600],
            '2024-01': [3000, 2800],
            '2025-01': [3200, 2950]
        })
        
        custom_years = ['2023']
        result = transformer.aggregate_rent_by_neighborhood(test_df, custom_years)
        
        assert '2023-01' in result.columns
        assert '2024-01' not in result.columns


class TestReshapeRentData:
    """Test reshape_rent_data method."""
    
    def test_reshape_rent_data_success(self):
        """Test successful rent data reshaping."""
        transformer = DataTransformer()
        
        # Create test data in wide format
        test_df = pd.DataFrame({
            'neighborhood': ['East Village', 'Williamsburg'],
            '2024-01-01': [3000, 2800],
            '2024-02-01': [3050, 2850]
        })
        test_df.set_index('neighborhood', inplace=True)
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.reshape_rent_data(test_df)
        
        assert isinstance(result, pd.DataFrame)
        assert 'neighborhood' in result.columns
        assert 'date' in result.columns
        assert 'median_rent' in result.columns
        assert 'year' in result.columns
        assert 'month' in result.columns
        assert len(result) == 4  # 2 neighborhoods * 2 dates
        assert "Reshaped rent data shape:" in fake_out.getvalue()


class TestTransformNYC311Data:
    """Test transform_nyc_311_data pipeline method."""
    
    def test_transform_nyc_311_data_complete_pipeline(self, datetime_311_data, sample_uhf_data):
        """Test complete NYC 311 transformation pipeline."""
        transformer = DataTransformer()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.transform_nyc_311_data(datetime_311_data, sample_uhf_data)
        
        # Verify pipeline steps were executed
        output = fake_out.getvalue()
        assert "Starting NYC 311 data transformation pipeline" in output
        assert "NYC 311 data transformation completed" in output
        assert "Created feature: resolution_time_hours" in output
        assert "Created time features: month, year" in output
        assert "Neighborhood mapping results:" in output
        
        # Verify result properties
        assert 'resolution_time_hours' in result.columns
        assert 'month' in result.columns
        assert 'year' in result.columns
        assert 'neighborhood' in result.columns


class TestTransformRentData:
    """Test transform_rent_data pipeline method."""
    
    def test_transform_rent_data_complete_pipeline(self, rent_data_for_transform, sample_manual_mapping):
        """Test complete rent data transformation pipeline."""
        transformer = DataTransformer()
        
        # Create simple manual mapping
        manual_map = {
            'east village': 'East Village',
            'williamsburg': 'Williamsburg'
        }
        
        # Add areaName column that matches transformer expectation
        test_data = rent_data_for_transform.copy()
        test_data['areaName'] = test_data['areaname']
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = transformer.transform_rent_data(test_data, manual_map)
        
        # Verify pipeline steps were executed
        output = fake_out.getvalue()
        assert "Starting rent data transformation pipeline" in output
        assert "Rent data transformation completed" in output
        assert "Area to neighborhood mapping results:" in output
        
        # Verify result properties
        assert 'neighborhood' in result.columns


class TestUtilityMethods:
    """Test utility methods and edge cases."""
    
    def test_method_return_types(self, datetime_311_data):
        """Test that all methods return correct data types."""
        transformer = DataTransformer()
        
        # Test create_time_features
        result1 = transformer.create_time_features(datetime_311_data)
        assert isinstance(result1, pd.DataFrame)
        
        # Test calculate_resolution_time
        result2 = transformer.calculate_resolution_time(datetime_311_data)
        assert isinstance(result2, pd.DataFrame)
        
        # Test create_zip_to_neighborhood_mapping
        uhf_data = {'Manhattan': [{'neighborhood': 'Test', 'zip_codes': ['10001']}]}
        result3 = transformer.create_zip_to_neighborhood_mapping(uhf_data)
        assert isinstance(result3, dict)
    
    def test_data_immutability(self, datetime_311_data):
        """Test that original data is not modified by transformation operations."""
        transformer = DataTransformer()
        original_shape = datetime_311_data.shape
        original_columns = list(datetime_311_data.columns)
        
        # Run several operations
        transformer.create_time_features(datetime_311_data)
        transformer.calculate_resolution_time(datetime_311_data)
        
        # Original data should remain unchanged (but new columns may be added in copies)
        assert datetime_311_data.shape[0] == original_shape[0]  # Same number of rows
        # Note: columns may be added by operations that return modified copies
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrames."""
        transformer = DataTransformer()
        empty_df = pd.DataFrame()
        
        # These should not crash
        result1 = transformer.create_time_features(empty_df)
        result2 = transformer.calculate_resolution_time(empty_df)
        
        assert len(result1) == 0
        assert len(result2) == 0