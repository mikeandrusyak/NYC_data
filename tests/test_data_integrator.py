"""
Unit tests for DataIntegrator class.

This module contains comprehensive tests for the DataIntegrator component
covering data merging, integration, validation, optimization, and export functionality.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, mock_open
from io import StringIO
import tempfile
import os

from daw_nyc.libs.daw.data_integrator import DataIntegrator


class TestDataIntegratorInitialization:
    """Test DataIntegrator initialization."""
    
    def test_init_default(self):
        """Test DataIntegrator initialization with default parameters."""
        integrator = DataIntegrator()
        assert integrator is not None
        assert isinstance(integrator, DataIntegrator)


class TestMergeComplaintsAndRent:
    """Test merge_complaints_and_rent method."""
    
    def test_merge_complaints_and_rent_default_columns(self, aggregated_complaints_data, reshaped_rent_data):
        """Test merging complaints and rent data with default columns."""
        integrator = DataIntegrator()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = integrator.merge_complaints_and_rent(aggregated_complaints_data, reshaped_rent_data)
        
        # Check that merge was successful
        assert len(result) == len(aggregated_complaints_data)
        assert 'median_rent' in result.columns
        
        # Check output contains merge statistics
        output = fake_out.getvalue()
        assert "Final merged dataset shape:" in output
        assert "Records with rent data:" in output
        assert "Records without rent data:" in output
    
    def test_merge_complaints_and_rent_custom_columns(self, aggregated_complaints_data, reshaped_rent_data):
        """Test merging with custom merge columns."""
        integrator = DataIntegrator()
        custom_columns = ['neighborhood', 'year']
        
        # Modify test data to have different months for testing
        complaints_custom = aggregated_complaints_data.copy()
        complaints_custom['month'] = [2, 2, 2, 2, 2]  # Different months
        
        result = integrator.merge_complaints_and_rent(
            complaints_custom, reshaped_rent_data, 
            merge_columns=custom_columns
        )
        
        assert len(result) == len(complaints_custom)
        assert 'median_rent' in result.columns
    
    def test_merge_complaints_and_rent_inner_join(self, aggregated_complaints_data, reshaped_rent_data):
        """Test merging with inner join."""
        integrator = DataIntegrator()
        
        result = integrator.merge_complaints_and_rent(
            aggregated_complaints_data, reshaped_rent_data, how='inner'
        )
        
        # Inner join should only keep records with matching rent data
        assert len(result) <= len(aggregated_complaints_data)
        assert result['median_rent'].isna().sum() == 0  # No missing rent data in inner join
    
    def test_merge_complaints_and_rent_no_matches(self, aggregated_complaints_data):
        """Test merging when there are no matching records."""
        integrator = DataIntegrator()
        
        # Create rent data with no matching neighborhoods
        no_match_rent = pd.DataFrame({
            'neighborhood': ['Different Area 1', 'Different Area 2'],
            'year': [2024, 2024],
            'month': [1, 1],
            'median_rent': [5000.0, 6000.0]
        })
        
        result = integrator.merge_complaints_and_rent(aggregated_complaints_data, no_match_rent)
        
        # All rent values should be NaN since no matches
        assert result['median_rent'].isna().all()


class TestCreateFinalDataset:
    """Test create_final_dataset method."""
    
    def test_create_final_dataset_success(self, aggregated_complaints_data, reshaped_rent_data):
        """Test successful creation of final dataset."""
        integrator = DataIntegrator()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = integrator.create_final_dataset(aggregated_complaints_data, reshaped_rent_data)
        
        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'median_rent' in result.columns
        
        # Verify output messages
        output = fake_out.getvalue()
        assert "Creating final integrated dataset..." in output
        assert "Final dataset integration completed!" in output
    
    def test_create_final_dataset_calls_merge(self, aggregated_complaints_data, reshaped_rent_data):
        """Test that create_final_dataset calls merge_complaints_and_rent."""
        integrator = DataIntegrator()
        
        with patch.object(integrator, 'merge_complaints_and_rent') as mock_merge:
            mock_merge.return_value = pd.DataFrame({'test': [1, 2, 3]})
            
            result = integrator.create_final_dataset(aggregated_complaints_data, reshaped_rent_data)
            
            mock_merge.assert_called_once_with(aggregated_complaints_data, reshaped_rent_data)
            assert list(result.columns) == ['test']


class TestExportDataset:
    """Test export_dataset method."""
    
    def test_export_dataset_success(self, final_integrated_dataset):
        """Test successful dataset export."""
        integrator = DataIntegrator()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            with patch('sys.stdout', new=StringIO()) as fake_out:
                integrator.export_dataset(final_integrated_dataset, temp_path)
            
            # Verify file was created and contains data
            assert os.path.exists(temp_path)
            exported_df = pd.read_csv(temp_path)
            assert len(exported_df) == len(final_integrated_dataset)
            assert list(exported_df.columns) == list(final_integrated_dataset.columns)
            
            # Verify output messages
            output = fake_out.getvalue()
            assert f"Dataset exported to: {temp_path}" in output
            assert "Dataset shape:" in output
            
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_export_dataset_with_mocks(self, final_integrated_dataset):
        """Test dataset export with mocked file operations."""
        integrator = DataIntegrator()
        
        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            with patch('sys.stdout', new=StringIO()) as fake_out:
                integrator.export_dataset(final_integrated_dataset, 'test_output.csv')
            
            mock_to_csv.assert_called_once_with('test_output.csv', index=False)
            
            output = fake_out.getvalue()
            assert "Dataset exported to: test_output.csv" in output


class TestGetDatasetSummary:
    """Test get_dataset_summary method."""
    
    def test_get_dataset_summary_structure(self, final_integrated_dataset):
        """Test that get_dataset_summary returns correct structure."""
        integrator = DataIntegrator()
        
        summary = integrator.get_dataset_summary(final_integrated_dataset)
        
        # Verify summary structure
        assert isinstance(summary, dict)
        expected_keys = ['total_records', 'date_range', 'unique_neighborhoods', 
                        'unique_complaint_types', 'data_completeness']
        for key in expected_keys:
            assert key in summary
    
    def test_get_dataset_summary_values(self, final_integrated_dataset):
        """Test that get_dataset_summary calculates correct values."""
        integrator = DataIntegrator()
        
        summary = integrator.get_dataset_summary(final_integrated_dataset)
        
        # Verify calculated values
        assert summary['total_records'] == len(final_integrated_dataset)
        assert summary['unique_neighborhoods'] == final_integrated_dataset['neighborhood'].nunique()
        assert summary['unique_complaint_types'] == final_integrated_dataset['complaint_type'].nunique()
        assert '2024-2024' == summary['date_range']  # All data is from 2024
        
        # Verify data completeness is a dictionary with percentages
        assert isinstance(summary['data_completeness'], dict)
        for column in final_integrated_dataset.columns:
            assert column in summary['data_completeness']
            assert 0 <= summary['data_completeness'][column] <= 100


class TestPrintDatasetSummary:
    """Test print_dataset_summary method."""
    
    def test_print_dataset_summary_output(self, final_integrated_dataset):
        """Test that print_dataset_summary produces expected output."""
        integrator = DataIntegrator()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            integrator.print_dataset_summary(final_integrated_dataset)
        
        output = fake_out.getvalue()
        
        # Verify key sections are present
        assert "=== Final Dataset Summary ===" in output
        assert "=== Data Completeness ===" in output
        assert "=== Top 10 Neighborhoods by Complaint Volume ===" in output
        assert "Total records:" in output
        assert "Date range:" in output
        assert "Unique neighborhoods:" in output


class TestValidateMergedData:
    """Test validate_merged_data method."""
    
    def test_validate_merged_data_clean_dataset(self, final_integrated_dataset):
        """Test validation with clean dataset."""
        integrator = DataIntegrator()
        
        results = integrator.validate_merged_data(final_integrated_dataset)
        
        # Verify validation structure
        expected_checks = ['has_duplicates', 'missing_neighborhoods', 'missing_rent_data',
                          'invalid_years', 'invalid_months', 'negative_complaints', 
                          'negative_resolution_time']
        for check in expected_checks:
            assert check in results
        
        # Clean dataset should pass most validations
        assert results['has_duplicates'] == False
        assert results['missing_neighborhoods'] == 0
        assert results['negative_complaints'] == 0
    
    def test_validate_merged_data_problematic_dataset(self, dataset_with_data_quality_issues):
        """Test validation with dataset containing data quality issues."""
        integrator = DataIntegrator()
        
        results = integrator.validate_merged_data(dataset_with_data_quality_issues)
        
        # This dataset has various issues that should be detected
        assert results['missing_neighborhoods'] > 0
        assert results['missing_rent_data'] > 0
        assert results['invalid_years'] > 0
        assert results['invalid_months'] > 0
        assert results['negative_complaints'] > 0
        assert results['negative_resolution_time'] > 0


class TestPrintValidationResults:
    """Test print_validation_results method."""
    
    def test_print_validation_results_output(self, final_integrated_dataset):
        """Test that print_validation_results produces expected output."""
        integrator = DataIntegrator()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            integrator.print_validation_results(final_integrated_dataset)
        
        output = fake_out.getvalue()
        
        # Verify validation output format
        assert "=== Data Validation Results ===" in output
        assert "✓ PASS" in output or "✗ FAIL" in output


class TestGetSampleData:
    """Test get_sample_data method."""
    
    def test_get_sample_data_default_size(self, final_integrated_dataset):
        """Test getting sample data with default size."""
        integrator = DataIntegrator()
        
        sample = integrator.get_sample_data(final_integrated_dataset)
        
        assert isinstance(sample, pd.DataFrame)
        assert len(sample) == min(10, len(final_integrated_dataset))
        assert list(sample.columns) == list(final_integrated_dataset.columns)
    
    def test_get_sample_data_custom_size(self, final_integrated_dataset):
        """Test getting sample data with custom size."""
        integrator = DataIntegrator()
        custom_size = 2
        
        sample = integrator.get_sample_data(final_integrated_dataset, n=custom_size)
        
        assert len(sample) == min(custom_size, len(final_integrated_dataset))


class TestOptimizeDataTypes:
    """Test optimize_data_types and related methods."""
    
    def test_optimize_data_types_output(self, dataset_for_optimization):
        """Test that optimize_data_types produces expected output and results."""
        integrator = DataIntegrator()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            optimized_df = integrator.optimize_data_types(dataset_for_optimization)
        
        output = fake_out.getvalue()
        
        # Verify optimization output
        assert "=== OPTIMIZING DATA TYPES ===" in output
        assert "📊 Analyzing and converting categorical columns..." in output
        assert "🔢 Analyzing and optimizing numeric columns..." in output
        assert "✅ Optimization complete!" in output
        assert "📊 MEMORY OPTIMIZATION RESULTS:" in output
        
        # Verify result is DataFrame with same shape
        assert isinstance(optimized_df, pd.DataFrame)
        assert optimized_df.shape == dataset_for_optimization.shape
    
    def test_detect_categorical_columns(self, dataset_for_optimization):
        """Test _detect_categorical_columns method."""
        integrator = DataIntegrator()
        
        categorical_columns = integrator._detect_categorical_columns(dataset_for_optimization)
        
        # Should detect neighborhood and complaint_type as categorical
        assert 'neighborhood' in categorical_columns
        assert 'complaint_type' in categorical_columns
        
        # Should not include numeric columns
        assert 'year' not in categorical_columns
        assert 'complaint_count' not in categorical_columns
    
    def test_optimize_numeric_columns(self, dataset_for_optimization):
        """Test _optimize_numeric_columns method."""
        integrator = DataIntegrator()
        
        optimizations = integrator._optimize_numeric_columns(dataset_for_optimization)
        
        # Should suggest optimizations for large numeric types
        assert isinstance(optimizations, dict)
        
        # Check that it identifies optimization opportunities
        for col, (original_type, new_type, reason) in optimizations.items():
            assert isinstance(original_type, str)
            assert isinstance(new_type, str)
            assert isinstance(reason, str)
    
    def test_optimize_integer_column(self):
        """Test _optimize_integer_column method."""
        integrator = DataIntegrator()
        
        # Test small positive range
        small_series = pd.Series([1, 2, 3, 4, 5])
        opt_type, reason = integrator._optimize_integer_column(small_series)
        assert opt_type == 'uint8'
        assert 'uint8' in reason
        
        # Test with negative values
        negative_series = pd.Series([-5, -1, 0, 1, 5])
        opt_type, reason = integrator._optimize_integer_column(negative_series)
        assert opt_type == 'int8'
        assert 'int8' in reason
    
    def test_optimize_float_column(self):
        """Test _optimize_float_column method."""
        integrator = DataIntegrator()
        
        float_series = pd.Series([1.1, 2.2, 3.3, 4.4, 5.5])
        opt_type, reason = integrator._optimize_float_column(float_series)
        
        assert opt_type == 'float32'
        assert 'precision' in reason.lower() and 'memory' in reason.lower()


class TestUtilityMethods:
    """Test utility methods and edge cases."""
    
    def test_method_return_types(self, aggregated_complaints_data, reshaped_rent_data):
        """Test that all methods return correct data types."""
        integrator = DataIntegrator()
        
        # Test merge method returns DataFrame
        result1 = integrator.merge_complaints_and_rent(aggregated_complaints_data, reshaped_rent_data)
        assert isinstance(result1, pd.DataFrame)
        
        # Test summary method returns dict
        result2 = integrator.get_dataset_summary(result1)
        assert isinstance(result2, dict)
        
        # Test validation method returns dict
        result3 = integrator.validate_merged_data(result1)
        assert isinstance(result3, dict)
        
        # Test sample method returns DataFrame
        result4 = integrator.get_sample_data(result1)
        assert isinstance(result4, pd.DataFrame)
    
    def test_empty_dataset_handling(self):
        """Test handling of empty datasets."""
        integrator = DataIntegrator()
        
        empty_complaints = pd.DataFrame(columns=['neighborhood', 'year', 'month', 'complaint_count'])
        empty_rent = pd.DataFrame(columns=['neighborhood', 'year', 'month', 'median_rent'])
        
        result = integrator.merge_complaints_and_rent(empty_complaints, empty_rent)
        assert len(result) == 0
        assert isinstance(result, pd.DataFrame)
    
    def test_data_immutability(self, aggregated_complaints_data, reshaped_rent_data):
        """Test that original data is not modified by integration operations."""
        integrator = DataIntegrator()
        
        # Store original shapes and data
        original_complaints_shape = aggregated_complaints_data.shape
        original_rent_shape = reshaped_rent_data.shape
        original_complaints_columns = list(aggregated_complaints_data.columns)
        
        # Run integration operations that don't require merged dataset
        merged_result = integrator.merge_complaints_and_rent(aggregated_complaints_data, reshaped_rent_data)
        integrator.get_dataset_summary(merged_result)  # Use merged result for validation
        integrator.validate_merged_data(merged_result)  # Use merged result for validation
        
        # Original data should remain unchanged
        assert aggregated_complaints_data.shape == original_complaints_shape
        assert reshaped_rent_data.shape == original_rent_shape
        assert list(aggregated_complaints_data.columns) == original_complaints_columns