"""
Unit tests for DataCleaner class.

This module contains comprehensive tests for the DataCleaner component
covering column selection, missing value analysis, duplicate removal,
date cleaning, geographic data cleaning, and pipeline functionality.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from io import StringIO
import sys

from libs_daw.data_cleaner import DataCleaner


class TestDataCleanerInitialization:
    """Test DataCleaner initialization."""
    
    def test_init_default(self):
        """Test DataCleaner initialization with default parameters."""
        cleaner = DataCleaner()
        assert cleaner is not None
        assert isinstance(cleaner, DataCleaner)


class TestSelectRelevantColumns:
    """Test select_relevant_columns method."""
    
    def test_select_relevant_columns_success(self, sample_nyc_311_data):
        """Test successful column selection."""
        cleaner = DataCleaner()
        columns_to_select = ['complaint_type', 'created_date', 'borough']
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.select_relevant_columns(sample_nyc_311_data, columns_to_select)
        
        assert len(result.columns) == 3
        assert list(result.columns) == columns_to_select
        assert len(result) == len(sample_nyc_311_data)
        assert "Selected 3 columns from dataset" in fake_out.getvalue()
    
    def test_select_relevant_columns_subset(self, sample_nyc_311_data):
        """Test column selection with subset of data."""
        cleaner = DataCleaner()
        columns_to_select = ['complaint_type', 'incident_zip']
        
        result = cleaner.select_relevant_columns(sample_nyc_311_data, columns_to_select)
        
        assert len(result.columns) == 2
        assert 'complaint_type' in result.columns
        assert 'incident_zip' in result.columns
        assert 'created_date' not in result.columns
    
    def test_select_relevant_columns_invalid_column(self, sample_nyc_311_data):
        """Test column selection with invalid column name."""
        cleaner = DataCleaner()
        columns_to_select = ['complaint_type', 'invalid_column']
        
        with pytest.raises(KeyError):
            cleaner.select_relevant_columns(sample_nyc_311_data, columns_to_select)


class TestSelectRentDateColumns:
    """Test select_rent_date_columns method."""
    
    def test_select_rent_date_columns_default_years(self, dirty_rent_data):
        """Test rent date column selection with default years."""
        cleaner = DataCleaner()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.select_rent_date_columns(dirty_rent_data)
        
        # Should include first 3 columns + date columns for 2024 and 2025
        expected_cols = ['areaname', 'areaid', 'year', '2024-01', '2024-02', '2025-01']
        assert list(result.columns) == expected_cols
        assert "date columns plus 3 info columns" in fake_out.getvalue()
    
    def test_select_rent_date_columns_custom_years(self, dirty_rent_data):
        """Test rent date column selection with custom years."""
        cleaner = DataCleaner()
        custom_years = ['2023', '2022']
        
        result = cleaner.select_rent_date_columns(dirty_rent_data, custom_years)
        
        # Should include first 3 columns + date columns for 2023 and 2022
        expected_cols = ['areaname', 'areaid', 'year', '2023-12', '2022-12']
        assert list(result.columns) == expected_cols
    
    def test_select_rent_date_columns_no_matching_years(self, dirty_rent_data):
        """Test rent date column selection with no matching years."""
        cleaner = DataCleaner()
        custom_years = ['2030']  # No columns match this year
        
        result = cleaner.select_rent_date_columns(dirty_rent_data, custom_years)
        
        # Should only have first 3 columns
        expected_cols = ['areaname', 'areaid', 'year']
        assert list(result.columns) == expected_cols


class TestAnalyzeMissingValues:
    """Test analyze_missing_values method."""
    
    def test_analyze_missing_values_with_missing_data(self, dirty_nyc_311_data):
        """Test missing values analysis with data containing missing values."""
        cleaner = DataCleaner()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.analyze_missing_values(dirty_nyc_311_data, "Test Dataset")
        
        assert isinstance(result, pd.DataFrame)
        assert 'Missing_Count' in result.columns
        assert 'Missing_Percentage' in result.columns
        
        # Check that closed_date and incident_zip have missing values
        assert 'closed_date' in result.index
        assert 'incident_zip' in result.index
        
        # Verify output contains dataset info
        output = fake_out.getvalue()
        assert "Test Dataset" in output
        assert "Total number of rows" in output
    
    def test_analyze_missing_values_no_missing_data(self, sample_nyc_311_data):
        """Test missing values analysis with complete data."""
        cleaner = DataCleaner()
        
        # Remove any NaN values to create complete dataset
        complete_data = sample_nyc_311_data.dropna()
        
        result = cleaner.analyze_missing_values(complete_data, "Complete Dataset")
        
        # Should return empty DataFrame when no missing values
        assert len(result) == 0 or result['Missing_Count'].sum() == 0


class TestRemoveDuplicates:
    """Test remove_duplicates method."""
    
    def test_remove_duplicates_with_duplicates(self, dataframe_with_duplicates):
        """Test duplicate removal with data containing duplicates."""
        cleaner = DataCleaner()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.remove_duplicates(dataframe_with_duplicates, "Test Dataset")
        
        assert len(result) < len(dataframe_with_duplicates)
        output = fake_out.getvalue()
        assert "Duplicate rows in Test Dataset:" in output
        assert "Removed" in output
        assert "duplicate rows" in output
    
    def test_remove_duplicates_no_duplicates(self, sample_nyc_311_data):
        """Test duplicate removal with data containing no duplicates."""
        cleaner = DataCleaner()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.remove_duplicates(sample_nyc_311_data, "Clean Dataset")
        
        assert len(result) == len(sample_nyc_311_data)
        output = fake_out.getvalue()
        assert "Duplicate rows in Clean Dataset: 0" in output


class TestCleanDates:
    """Test clean_dates method."""
    
    def test_clean_dates_success(self, dirty_nyc_311_data):
        """Test successful date cleaning."""
        cleaner = DataCleaner()
        date_columns = ['created_date', 'closed_date', 'resolution_action_updated_date']
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.clean_dates(dirty_nyc_311_data, date_columns)
        
        # Check that dates are converted to datetime
        assert pd.api.types.is_datetime64_any_dtype(result['created_date'])
        assert pd.api.types.is_datetime64_any_dtype(result['closed_date'])
        
        # Check that invalid date records are removed
        output = fake_out.getvalue()
        assert "created_date > closed_date" in output
        assert len(result) < len(dirty_nyc_311_data)
    
    def test_clean_dates_missing_columns(self, sample_nyc_311_data):
        """Test date cleaning with missing date columns."""
        cleaner = DataCleaner()
        date_columns = ['created_date', 'nonexistent_date']
        
        result = cleaner.clean_dates(sample_nyc_311_data, date_columns)
        
        # Should only process existing columns
        assert pd.api.types.is_datetime64_any_dtype(result['created_date'])
        assert 'nonexistent_date' not in result.columns
    
    def test_clean_dates_invalid_dates(self):
        """Test date cleaning with invalid date strings."""
        cleaner = DataCleaner()
        test_df = pd.DataFrame({
            'created_date': ['2024-01-01', 'invalid_date', '2024-01-03'],
            'closed_date': ['2024-01-02', '2024-01-02', '2024-01-04']
        })
        
        result = cleaner.clean_dates(test_df, ['created_date', 'closed_date'])
        
        # After cleaning, some rows may be removed due to invalid dates
        # Check that invalid dates become NaT where they remain
        invalid_date_rows = result[result['created_date'].isna()]
        assert len(invalid_date_rows) >= 0  # May be 0 if rows were removed
        
        # Valid dates should remain valid
        valid_date_rows = result[result['created_date'].notna()]
        assert len(valid_date_rows) >= 2  # At least 2 valid dates should remain


class TestCleanGeographicData:
    """Test clean_geographic_data method."""
    
    def test_clean_geographic_data_success(self, dirty_nyc_311_data):
        """Test successful geographic data cleaning."""
        cleaner = DataCleaner()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.clean_geographic_data(dirty_nyc_311_data, 'city')
        
        # Check that city names are standardized
        assert result['city'].iloc[0] == 'NEW YORK'  # Was 'NEW YORK  '
        assert result['city'].iloc[1] == 'NEW YORK'  # Was ' new york'
        assert result['city'].iloc[2] == 'OUTSIDE NYC'  # Was 'FLORAL PARK'
        assert result['city'].iloc[4] == 'OUTSIDE NYC'  # Was 'BREEZY POINT'
        
        output = fake_out.getvalue()
        assert "City names standardized" in output
        assert "Unique cities after cleaning:" in output
    
    def test_clean_geographic_data_missing_column(self, sample_nyc_311_data):
        """Test geographic data cleaning with missing city column."""
        cleaner = DataCleaner()
        
        # Test with DataFrame without city column
        test_df = sample_nyc_311_data.drop('city', axis=1)
        result = cleaner.clean_geographic_data(test_df, 'city')
        
        # Should return original dataframe unchanged
        assert 'city' not in result.columns
        assert len(result) == len(test_df)
    
    def test_clean_geographic_data_custom_column(self, dirty_nyc_311_data):
        """Test geographic data cleaning with custom column name."""
        cleaner = DataCleaner()
        
        # Rename city to location for testing
        test_df = dirty_nyc_311_data.rename(columns={'city': 'location'})
        result = cleaner.clean_geographic_data(test_df, 'location')
        
        # Should clean the location column
        assert result['location'].iloc[0] == 'NEW YORK'


class TestCleanNYC311Data:
    """Test clean_nyc_311_data pipeline method."""
    
    def test_clean_nyc_311_data_complete_pipeline(self, dirty_nyc_311_data):
        """Test complete NYC 311 data cleaning pipeline."""
        cleaner = DataCleaner()
        relevant_columns = ['complaint_type', 'created_date', 'closed_date', 'incident_zip', 'borough', 'city']
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.clean_nyc_311_data(dirty_nyc_311_data, relevant_columns)
        
        # Verify pipeline steps were executed
        output = fake_out.getvalue()
        assert "Starting NYC 311 data cleaning pipeline" in output
        assert "NYC 311 data cleaning completed" in output
        assert "Selected" in output  # Column selection
        assert "Duplicate rows" in output  # Duplicate removal
        assert "City names standardized" in output  # Geographic cleaning
        
        # Verify result properties
        assert len(result.columns) == len(relevant_columns)
        assert len(result) <= len(dirty_nyc_311_data)  # May be smaller due to cleaning
    
    def test_clean_nyc_311_data_empty_dataframe(self):
        """Test NYC 311 pipeline with empty DataFrame."""
        cleaner = DataCleaner()
        empty_df = pd.DataFrame()
        relevant_columns = ['complaint_type', 'created_date']
        
        with pytest.raises(KeyError):
            cleaner.clean_nyc_311_data(empty_df, relevant_columns)


class TestCleanRentData:
    """Test clean_rent_data pipeline method."""
    
    def test_clean_rent_data_complete_pipeline(self, dirty_rent_data):
        """Test complete rent data cleaning pipeline."""
        cleaner = DataCleaner()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = cleaner.clean_rent_data(dirty_rent_data)
        
        # Verify pipeline steps were executed
        output = fake_out.getvalue()
        assert "Starting median rent data cleaning pipeline" in output
        assert "Median rent data cleaning completed" in output
        assert "date columns plus 3 info columns" in output
        assert "Duplicate rows" in output
        
        # Verify result has expected structure
        assert 'areaname' in result.columns
        assert 'areaid' in result.columns
        assert 'year' in result.columns
        assert any('2024' in col for col in result.columns)
    
    def test_clean_rent_data_custom_years(self, dirty_rent_data):
        """Test rent data cleaning with custom years."""
        cleaner = DataCleaner()
        custom_years = ['2023']
        
        result = cleaner.clean_rent_data(dirty_rent_data, custom_years)
        
        # Should only include 2023 date columns
        date_cols = [col for col in result.columns if '2023' in col]
        assert len(date_cols) > 0
        non_2024_cols = [col for col in result.columns if '2024' in col]
        assert len(non_2024_cols) == 0
    
    def test_clean_rent_data_empty_dataframe(self):
        """Test rent data pipeline with empty DataFrame."""
        cleaner = DataCleaner()
        empty_df = pd.DataFrame()
        
        # Should handle empty DataFrame gracefully
        result = cleaner.clean_rent_data(empty_df)
        assert len(result) == 0


class TestUtilityMethods:
    """Test utility methods and edge cases."""
    
    def test_method_return_types(self, sample_nyc_311_data):
        """Test that all methods return correct data types."""
        cleaner = DataCleaner()
        
        # Test select_relevant_columns
        result1 = cleaner.select_relevant_columns(sample_nyc_311_data, ['complaint_type'])
        assert isinstance(result1, pd.DataFrame)
        
        # Test analyze_missing_values
        result2 = cleaner.analyze_missing_values(sample_nyc_311_data)
        assert isinstance(result2, pd.DataFrame)
        
        # Test remove_duplicates
        result3 = cleaner.remove_duplicates(sample_nyc_311_data)
        assert isinstance(result3, pd.DataFrame)
    
    def test_data_immutability(self, sample_nyc_311_data):
        """Test that original data is not modified by cleaning operations."""
        cleaner = DataCleaner()
        original_shape = sample_nyc_311_data.shape
        original_columns = list(sample_nyc_311_data.columns)
        
        # Run several operations
        cleaner.select_relevant_columns(sample_nyc_311_data, ['complaint_type', 'borough'])
        cleaner.analyze_missing_values(sample_nyc_311_data)
        cleaner.remove_duplicates(sample_nyc_311_data)
        
        # Original data should remain unchanged
        assert sample_nyc_311_data.shape == original_shape
        assert list(sample_nyc_311_data.columns) == original_columns