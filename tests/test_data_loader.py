"""
Unit tests for DataLoader class.

Tests the DataLoader functionality using mocks and synthetic data,
ensuring tests are fast and independent of real data files.
"""

import pytest
import pandas as pd
import json
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path

from daw_nyc.libs.daw.data_loader import DataLoader
from tests.conftest import (
    create_sample_311_dataframe,
    create_sample_rent_dataframe,
    create_sample_uhf_mapping,
    create_sample_manual_mapping
)


class TestDataLoaderInitialization:
    """Test DataLoader initialization."""
    
    def test_init_default_base_path(self):
        """Test DataLoader initialization with default base path."""
        loader = DataLoader()
        assert loader.base_path == "."
    
    def test_init_custom_base_path(self):
        """Test DataLoader initialization with custom base path."""
        custom_path = "/custom/path"
        loader = DataLoader(base_path=custom_path)
        assert loader.base_path == custom_path


class TestLoadNYC311Data:
    """Test loading NYC 311 data."""
    
    @patch('pandas.read_csv')
    def test_load_nyc_311_data_success(self, mock_read_csv, sample_nyc_311_data):
        """Test successful loading of NYC 311 data."""
        mock_read_csv.return_value = sample_nyc_311_data
        
        loader = DataLoader()
        result = loader.load_nyc_311_data()
        
        # Verify pandas.read_csv was called correctly
        mock_read_csv.assert_called_once_with("data/nyc_311_2024_2025_sample.csv", index_col="unique_key")
        
        # Verify result
        assert isinstance(result, pd.DataFrame)
        assert result.index.name == "unique_key"
        assert len(result) == 5
        assert 'complaint_type' in result.columns
    
    @patch('pandas.read_csv')
    def test_load_nyc_311_data_custom_path(self, mock_read_csv, sample_nyc_311_data):
        """Test loading NYC 311 data with custom file path."""
        mock_read_csv.return_value = sample_nyc_311_data
        
        loader = DataLoader()
        custom_path = "custom/path/data.csv"
        result = loader.load_nyc_311_data(file_path=custom_path)
        
        mock_read_csv.assert_called_once_with(custom_path, index_col="unique_key")
        assert isinstance(result, pd.DataFrame)
    
    @patch('pandas.read_csv')
    def test_load_nyc_311_data_with_base_path(self, mock_read_csv, sample_nyc_311_data):
        """Test loading NYC 311 data with custom base path."""
        mock_read_csv.return_value = sample_nyc_311_data
        
        loader = DataLoader(base_path="/base")
        result = loader.load_nyc_311_data()
        
        mock_read_csv.assert_called_once_with("/base/data/nyc_311_2024_2025_sample.csv", index_col="unique_key")
    
    @patch('pandas.read_csv')
    def test_load_nyc_311_data_file_not_found(self, mock_read_csv):
        """Test handling of file not found error."""
        mock_read_csv.side_effect = FileNotFoundError("File not found")
        
        loader = DataLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_nyc_311_data()
    
    @patch('pandas.read_csv')
    def test_load_nyc_311_data_empty_file(self, mock_read_csv):
        """Test handling of empty CSV file."""
        mock_read_csv.return_value = pd.DataFrame()
        
        loader = DataLoader()
        result = loader.load_nyc_311_data()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestLoadMedianRentData:
    """Test loading median rent data."""
    
    @patch('pandas.read_csv')
    def test_load_median_rent_data_success(self, mock_read_csv, sample_rent_data):
        """Test successful loading of rent data."""
        mock_read_csv.return_value = sample_rent_data
        
        loader = DataLoader()
        result = loader.load_median_rent_data()
        
        mock_read_csv.assert_called_once_with("data/medianAskingRent_All.csv")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert 'areaname' in result.columns
        assert 'rent_2024' in result.columns
    
    @patch('pandas.read_csv')
    def test_load_median_rent_data_custom_path(self, mock_read_csv, sample_rent_data):
        """Test loading rent data with custom file path."""
        mock_read_csv.return_value = sample_rent_data
        
        loader = DataLoader()
        custom_path = "custom/rent_data.csv"
        result = loader.load_median_rent_data(file_path=custom_path)
        
        mock_read_csv.assert_called_once_with(custom_path)
    
    @patch('pandas.read_csv')
    def test_load_median_rent_data_with_base_path(self, mock_read_csv, sample_rent_data):
        """Test loading rent data with custom base path."""
        mock_read_csv.return_value = sample_rent_data
        
        loader = DataLoader(base_path="/custom/base")
        result = loader.load_median_rent_data()
        
        mock_read_csv.assert_called_once_with("/custom/base/data/medianAskingRent_All.csv")
    
    @patch('pandas.read_csv')
    def test_load_median_rent_data_file_not_found(self, mock_read_csv):
        """Test handling of file not found error for rent data."""
        mock_read_csv.side_effect = FileNotFoundError("File not found")
        
        loader = DataLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_median_rent_data()


class TestLoadUHFMapping:
    """Test loading UHF mapping data."""
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_uhf_mapping_success(self, mock_json_load, mock_file, sample_uhf_mapping):
        """Test successful loading of UHF mapping."""
        mock_json_load.return_value = sample_uhf_mapping
        
        loader = DataLoader()
        result = loader.load_uhf_mapping()
        
        mock_file.assert_called_once_with("mappings/nyc_uhf_zipcodes.json", 'r')
        assert isinstance(result, dict)
        assert 'data' in result
        assert len(result['data']) == 5
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_uhf_mapping_custom_path(self, mock_json_load, mock_file, sample_uhf_mapping):
        """Test loading UHF mapping with custom file path."""
        mock_json_load.return_value = sample_uhf_mapping
        
        loader = DataLoader()
        custom_path = "custom/uhf.json"
        result = loader.load_uhf_mapping(file_path=custom_path)
        
        mock_file.assert_called_once_with(custom_path, 'r')
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_uhf_mapping_with_base_path(self, mock_json_load, mock_file, sample_uhf_mapping):
        """Test loading UHF mapping with custom base path."""
        mock_json_load.return_value = sample_uhf_mapping
        
        loader = DataLoader(base_path="/test/base")
        result = loader.load_uhf_mapping()
        
        mock_file.assert_called_once_with("/test/base/mappings/nyc_uhf_zipcodes.json", 'r')
    
    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    def test_load_uhf_mapping_file_not_found(self, mock_file):
        """Test handling of file not found error for UHF mapping."""
        loader = DataLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_uhf_mapping()
    
    @patch('builtins.open', new_callable=mock_open, read_data='{"invalid": json}')
    def test_load_uhf_mapping_invalid_json(self, mock_file):
        """Test handling of invalid JSON in UHF mapping."""
        loader = DataLoader()
        
        with pytest.raises(json.JSONDecodeError):
            loader.load_uhf_mapping()


class TestLoadManualMapping:
    """Test loading manual mapping data."""
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_manual_mapping_success(self, mock_json_load, mock_file, sample_manual_mapping):
        """Test successful loading of manual mapping."""
        mock_json_load.return_value = sample_manual_mapping
        
        loader = DataLoader()
        result = loader.load_manual_mapping()
        
        mock_file.assert_called_once_with("mappings/manual_map.json", 'r')
        assert isinstance(result, dict)
        assert len(result) == 5
        assert 'East Village' in result
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.load')
    def test_load_manual_mapping_custom_path(self, mock_json_load, mock_file, sample_manual_mapping):
        """Test loading manual mapping with custom file path."""
        mock_json_load.return_value = sample_manual_mapping
        
        loader = DataLoader()
        custom_path = "custom/manual.json"
        result = loader.load_manual_mapping(file_path=custom_path)
        
        mock_file.assert_called_once_with(custom_path, 'r')
    
    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    def test_load_manual_mapping_file_not_found(self, mock_file):
        """Test handling of file not found error for manual mapping."""
        loader = DataLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_manual_mapping()


class TestLoadAllData:
    """Test loading all data at once."""
    
    @patch.object(DataLoader, 'load_nyc_311_data')
    @patch.object(DataLoader, 'load_median_rent_data')
    @patch.object(DataLoader, 'load_uhf_mapping')
    @patch.object(DataLoader, 'load_manual_mapping')
    def test_load_all_data_success(self, mock_manual, mock_uhf, mock_rent, mock_311,
                                   sample_nyc_311_data, sample_rent_data, 
                                   sample_uhf_mapping, sample_manual_mapping):
        """Test successful loading of all data."""
        # Setup mocks
        mock_311.return_value = sample_nyc_311_data
        mock_rent.return_value = sample_rent_data
        mock_uhf.return_value = sample_uhf_mapping
        mock_manual.return_value = sample_manual_mapping
        
        loader = DataLoader()
        df_311, df_rent, uhf_data, manual_map = loader.load_all_data()
        
        # Verify all methods were called
        mock_311.assert_called_once()
        mock_rent.assert_called_once()
        mock_uhf.assert_called_once()
        mock_manual.assert_called_once()
        
        # Verify return types
        assert isinstance(df_311, pd.DataFrame)
        assert isinstance(df_rent, pd.DataFrame)
        assert isinstance(uhf_data, dict)
        assert isinstance(manual_map, dict)
    
    @patch.object(DataLoader, 'load_nyc_311_data')
    @patch.object(DataLoader, 'load_median_rent_data')
    @patch.object(DataLoader, 'load_uhf_mapping')
    @patch.object(DataLoader, 'load_manual_mapping')
    def test_load_all_data_partial_failure(self, mock_manual, mock_uhf, mock_rent, mock_311):
        """Test behavior when one data source fails to load."""
        # Setup mocks - one failing
        mock_311.side_effect = FileNotFoundError("311 file not found")
        mock_rent.return_value = create_sample_rent_dataframe()
        mock_uhf.return_value = create_sample_uhf_mapping()
        mock_manual.return_value = create_sample_manual_mapping()
        
        loader = DataLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_all_data()


class TestUtilityMethods:
    """Test utility methods of DataLoader."""
    
    def test_get_relevant_columns(self):
        """Test getting relevant columns list."""
        loader = DataLoader()
        columns = loader.get_relevant_columns()
        
        assert isinstance(columns, list)
        assert len(columns) > 0
        assert 'complaint_type' in columns
        assert 'created_date' in columns
        assert 'incident_zip' in columns
        assert 'borough' in columns
    
    @patch('builtins.print')
    def test_display_data_info(self, mock_print, sample_nyc_311_data, sample_rent_data):
        """Test displaying data information."""
        loader = DataLoader()
        
        # Should not raise any errors
        loader.display_data_info(sample_nyc_311_data, sample_rent_data)
        
        # Verify print was called
        assert mock_print.called
        
        # Verify that print was called multiple times (should display info about both datasets)
        assert mock_print.call_count > 1


class TestIntegrationWithTempFiles:
    """Integration tests using temporary files (faster than mocking for file I/O tests)."""
    
    @patch.object(DataLoader, 'load_nyc_311_data')
    @patch.object(DataLoader, 'load_median_rent_data')
    @patch.object(DataLoader, 'load_uhf_mapping')
    @patch.object(DataLoader, 'load_manual_mapping')
    def test_load_all_data_with_mocks(self, mock_manual, mock_uhf, mock_rent, mock_311,
                                      sample_nyc_311_data, sample_rent_data, 
                                      sample_uhf_mapping, sample_manual_mapping):
        """Test loading all data using mocks instead of temp files."""
        # Setup mocks
        mock_311.return_value = sample_nyc_311_data
        mock_rent.return_value = sample_rent_data
        mock_uhf.return_value = sample_uhf_mapping
        mock_manual.return_value = sample_manual_mapping
        
        loader = DataLoader()
        df_311, df_rent, uhf_data, manual_map = loader.load_all_data()
        
        # Verify data was loaded correctly
        assert isinstance(df_311, pd.DataFrame)
        assert len(df_311) == 5
        assert df_311.index.name == "unique_key"
        
        assert isinstance(df_rent, pd.DataFrame)
        assert len(df_rent) == 5
        
        assert isinstance(uhf_data, dict)
        assert 'data' in uhf_data
        
        assert isinstance(manual_map, dict)
        assert len(manual_map) == 5
    
    def test_load_nyc_311_with_mocks(self, sample_nyc_311_data):
        """Test loading NYC 311 data using mocks."""
        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = sample_nyc_311_data
            
            loader = DataLoader()
            df = loader.load_nyc_311_data()
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 5
            assert df.index.name == "unique_key"
            assert 'complaint_type' in df.columns
    
    def test_load_rent_with_mocks(self, sample_rent_data):
        """Test loading rent data using mocks."""
        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = sample_rent_data
            
            loader = DataLoader()
            df = loader.load_median_rent_data()
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 5
            assert 'areaname' in df.columns
            assert 'rent_2024' in df.columns