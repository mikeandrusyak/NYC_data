"""
Unit tests for NYCDataPipeline class.

This module contains comprehensive tests for the Pipeline component
covering end-to-end data processing workflow, step-by-step execution,
and integration between all components.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, mock_open
from io import StringIO
import tempfile
import os

from daw_nyc.libs.daw.pipeline import NYCDataPipeline
from daw_nyc.libs.daw.data_loader import DataLoader
from daw_nyc.libs.daw.data_cleaner import DataCleaner
from daw_nyc.libs.daw.data_transformer import DataTransformer
from daw_nyc.libs.daw.data_integrator import DataIntegrator


class TestNYCDataPipelineInitialization:
    """Test NYCDataPipeline initialization."""
    
    def test_init_default_base_path(self):
        """Test pipeline initialization with default base path."""
        pipeline = NYCDataPipeline()
        
        assert pipeline.base_path == "."
        assert isinstance(pipeline.loader, DataLoader)
        assert isinstance(pipeline.cleaner, DataCleaner)
        assert isinstance(pipeline.transformer, DataTransformer)
        assert isinstance(pipeline.integrator, DataIntegrator)
    
    def test_init_custom_base_path(self):
        """Test pipeline initialization with custom base path."""
        custom_path = "/custom/data/path"
        pipeline = NYCDataPipeline(base_path=custom_path)
        
        assert pipeline.base_path == custom_path
        assert pipeline.loader.base_path == custom_path


class TestLoadData:
    """Test load_data method."""
    
    def test_load_data_success(self, pipeline_test_data):
        """Test successful data loading."""
        pipeline = NYCDataPipeline()
        
        # Mock the loader.load_all_data method
        with patch.object(pipeline.loader, 'load_all_data') as mock_load:
            with patch.object(pipeline.loader, 'display_data_info') as mock_display:
                mock_load.return_value = (
                    pipeline_test_data['raw_nyc_311'],
                    pipeline_test_data['raw_rent'],
                    pipeline_test_data['uhf_mapping'],
                    pipeline_test_data['manual_mapping']
                )
                
                with patch('sys.stdout', new=StringIO()) as fake_out:
                    result = pipeline.load_data()
                
                # Verify method calls
                mock_load.assert_called_once()
                mock_display.assert_called_once()
                
                # Verify return values
                nyc_311, rent, uhf, manual = result
                assert isinstance(nyc_311, pd.DataFrame)
                assert isinstance(rent, pd.DataFrame)
                assert isinstance(uhf, dict)
                assert isinstance(manual, dict)
                
                # Verify output
                output = fake_out.getvalue()
                assert "STEP 1: LOADING DATA" in output
    
    def test_load_data_calls_components(self, pipeline_test_data):
        """Test that load_data calls the correct component methods."""
        pipeline = NYCDataPipeline()
        
        with patch.object(pipeline.loader, 'load_all_data') as mock_load:
            with patch.object(pipeline.loader, 'display_data_info') as mock_display:
                mock_load.return_value = (
                    pipeline_test_data['raw_nyc_311'],
                    pipeline_test_data['raw_rent'],
                    pipeline_test_data['uhf_mapping'],
                    pipeline_test_data['manual_mapping']
                )
                
                pipeline.load_data()
                
                # Verify that the loader methods are called with correct arguments
                mock_load.assert_called_once()
                mock_display.assert_called_once_with(
                    pipeline_test_data['raw_nyc_311'],
                    pipeline_test_data['raw_rent']
                )


class TestCleanData:
    """Test clean_data method."""
    
    def test_clean_data_success(self, pipeline_test_data, pipeline_intermediate_data):
        """Test successful data cleaning."""
        pipeline = NYCDataPipeline()
        
        with patch.object(pipeline.loader, 'get_relevant_columns') as mock_get_cols:
            with patch.object(pipeline.cleaner, 'clean_nyc_311_data') as mock_clean_311:
                with patch.object(pipeline.cleaner, 'clean_rent_data') as mock_clean_rent:
                    
                    mock_get_cols.return_value = ['complaint_type', 'created_date', 'borough']
                    mock_clean_311.return_value = pipeline_intermediate_data['cleaned_nyc_311']
                    mock_clean_rent.return_value = pipeline_intermediate_data['cleaned_rent']
                    
                    with patch('sys.stdout', new=StringIO()) as fake_out:
                        result = pipeline.clean_data(
                            pipeline_test_data['raw_nyc_311'],
                            pipeline_test_data['raw_rent']
                        )
                    
                    # Verify method calls
                    mock_get_cols.assert_called_once()
                    mock_clean_311.assert_called_once()
                    mock_clean_rent.assert_called_once()
                    
                    # Verify return values
                    cleaned_311, cleaned_rent = result
                    assert isinstance(cleaned_311, pd.DataFrame)
                    assert isinstance(cleaned_rent, pd.DataFrame)
                    
                    # Verify output
                    output = fake_out.getvalue()
                    assert "STEP 2: CLEANING DATA" in output
    
    def test_clean_data_component_integration(self, pipeline_test_data):
        """Test that clean_data properly integrates with cleaner components."""
        pipeline = NYCDataPipeline()
        
        with patch.object(pipeline.loader, 'get_relevant_columns') as mock_get_cols:
            with patch.object(pipeline.cleaner, 'clean_nyc_311_data') as mock_clean_311:
                with patch.object(pipeline.cleaner, 'clean_rent_data') as mock_clean_rent:
                    
                    relevant_columns = ['complaint_type', 'created_date']
                    mock_get_cols.return_value = relevant_columns
                    mock_clean_311.return_value = pd.DataFrame({'test': [1]})
                    mock_clean_rent.return_value = pd.DataFrame({'test': [2]})
                    
                    pipeline.clean_data(
                        pipeline_test_data['raw_nyc_311'],
                        pipeline_test_data['raw_rent']
                    )
                    
                    # Verify correct arguments passed to cleaning methods
                    mock_clean_311.assert_called_once_with(
                        pipeline_test_data['raw_nyc_311'], 
                        relevant_columns
                    )
                    mock_clean_rent.assert_called_once_with(
                        pipeline_test_data['raw_rent']
                    )


class TestTransformData:
    """Test transform_data method."""
    
    def test_transform_data_success(self, pipeline_test_data, pipeline_intermediate_data):
        """Test successful data transformation."""
        pipeline = NYCDataPipeline()
        
        with patch.object(pipeline.transformer, 'transform_nyc_311_data') as mock_transform_311:
            with patch.object(pipeline.transformer, 'transform_rent_data') as mock_transform_rent:
                
                mock_transform_311.return_value = pipeline_intermediate_data['transformed_nyc_311']
                mock_transform_rent.return_value = pipeline_intermediate_data['transformed_rent']
                
                with patch('sys.stdout', new=StringIO()) as fake_out:
                    result = pipeline.transform_data(
                        pipeline_intermediate_data['cleaned_nyc_311'],
                        pipeline_intermediate_data['cleaned_rent'],
                        pipeline_test_data['uhf_mapping'],
                        pipeline_test_data['manual_mapping']
                    )
                
                # Verify method calls
                mock_transform_311.assert_called_once()
                mock_transform_rent.assert_called_once()
                
                # Verify return values
                transformed_311, transformed_rent = result
                assert isinstance(transformed_311, pd.DataFrame)
                assert isinstance(transformed_rent, pd.DataFrame)
                
                # Verify output
                output = fake_out.getvalue()
                assert "STEP 3: TRANSFORMING DATA" in output
    
    def test_transform_data_component_integration(self, pipeline_test_data, pipeline_intermediate_data):
        """Test that transform_data properly integrates with transformer components."""
        pipeline = NYCDataPipeline()
        
        with patch.object(pipeline.transformer, 'transform_nyc_311_data') as mock_transform_311:
            with patch.object(pipeline.transformer, 'transform_rent_data') as mock_transform_rent:
                
                mock_transform_311.return_value = pd.DataFrame({'test': [1]})
                mock_transform_rent.return_value = pd.DataFrame({'test': [2]})
                
                pipeline.transform_data(
                    pipeline_intermediate_data['cleaned_nyc_311'],
                    pipeline_intermediate_data['cleaned_rent'],
                    pipeline_test_data['uhf_mapping'],
                    pipeline_test_data['manual_mapping']
                )
                
                # Verify correct arguments passed to transformation methods
                mock_transform_311.assert_called_once_with(
                    pipeline_intermediate_data['cleaned_nyc_311'],
                    pipeline_test_data['uhf_mapping']
                )
                mock_transform_rent.assert_called_once_with(
                    pipeline_intermediate_data['cleaned_rent'],
                    pipeline_test_data['manual_mapping'],
                    pipeline_test_data['uhf_mapping']
                )


class TestAggregateAndIntegrate:
    """Test aggregate_and_integrate method."""
    
    def test_aggregate_and_integrate_success(self, pipeline_intermediate_data):
        """Test successful aggregation and integration."""
        pipeline = NYCDataPipeline()
        
        # Mock all the transformer and integrator methods
        with patch.object(pipeline.transformer, 'aggregate_complaints_by_neighborhood') as mock_agg_complaints:
            with patch.object(pipeline.transformer, 'aggregate_rent_by_neighborhood') as mock_agg_rent:
                with patch.object(pipeline.transformer, 'reshape_rent_data') as mock_reshape:
                    with patch.object(pipeline.integrator, 'create_final_dataset') as mock_create_final:
                        with patch.object(pipeline.integrator, 'optimize_data_types') as mock_optimize:
                            with patch.object(pipeline.integrator, 'print_validation_results') as mock_validate:
                                with patch.object(pipeline.integrator, 'get_dataset_summary') as mock_summary:
                                    with patch.object(pipeline.integrator, 'export_dataset') as mock_export:
                                        
                                        # Setup mock returns
                                        mock_complaints_agg = pd.DataFrame({'neighborhood': ['Test'], 'count': [100]})
                                        mock_rent_agg = pd.DataFrame({'neighborhood': ['Test'], 'rent': [2500]})
                                        mock_rent_reshaped = pd.DataFrame({'neighborhood': ['Test'], 'rent': [2500]})
                                        mock_final = pd.DataFrame({'neighborhood': ['Test'], 'count': [100], 'rent': [2500]})
                                        mock_optimized = pd.DataFrame({'neighborhood': ['Test'], 'count': [100], 'rent': [2500]})
                                        
                                        mock_agg_complaints.return_value = mock_complaints_agg
                                        mock_agg_rent.return_value = mock_rent_agg
                                        mock_reshape.return_value = mock_rent_reshaped
                                        mock_create_final.return_value = mock_final
                                        mock_optimize.return_value = mock_optimized
                                        mock_summary.return_value = {'total_records': 1, 'neighborhoods': 1}
                                        
                                        with patch('sys.stdout', new=StringIO()) as fake_out:
                                            result = pipeline.aggregate_and_integrate(
                                                pipeline_intermediate_data['transformed_nyc_311'],
                                                pipeline_intermediate_data['transformed_rent']
                                            )
                                        
                                        # Verify all methods were called
                                        mock_agg_complaints.assert_called_once()
                                        mock_agg_rent.assert_called_once()
                                        mock_reshape.assert_called_once()
                                        mock_create_final.assert_called_once()
                                        mock_optimize.assert_called_once()
                                        mock_validate.assert_called_once()
                                        mock_summary.assert_called_once()
                                        mock_export.assert_called_once()
                                        
                                        # Verify return value
                                        assert isinstance(result, pd.DataFrame)
                                        
                                        # Verify output sections
                                        output = fake_out.getvalue()
                                        assert "STEP 4: AGGREGATION AND INTEGRATION" in output
                                        assert "DATA TYPE OPTIMIZATION" in output
                                        assert "DATA QUALITY VALIDATION" in output
                                        assert "DATASET SUMMARY" in output
    
    def test_aggregate_and_integrate_export_path(self, pipeline_intermediate_data):
        """Test that the correct export path is used."""
        pipeline = NYCDataPipeline()
        
        with patch.object(pipeline.transformer, 'aggregate_complaints_by_neighborhood'):
            with patch.object(pipeline.transformer, 'aggregate_rent_by_neighborhood'):
                with patch.object(pipeline.transformer, 'reshape_rent_data'):
                    with patch.object(pipeline.integrator, 'create_final_dataset') as mock_create:
                        with patch.object(pipeline.integrator, 'optimize_data_types') as mock_optimize:
                            with patch.object(pipeline.integrator, 'print_validation_results'):
                                with patch.object(pipeline.integrator, 'get_dataset_summary') as mock_summary:
                                    with patch.object(pipeline.integrator, 'export_dataset') as mock_export:
                                        
                                        mock_final = pd.DataFrame({'test': [1]})
                                        mock_create.return_value = mock_final
                                        mock_optimize.return_value = mock_final
                                        mock_summary.return_value = {}
                                        
                                        pipeline.aggregate_and_integrate(
                                            pipeline_intermediate_data['transformed_nyc_311'],
                                            pipeline_intermediate_data['transformed_rent']
                                        )
                                        
                                        # Verify export is called with correct path
                                        mock_export.assert_called_once_with(
                                            mock_final, 
                                            "data/data_snapshot_for_gdv.csv"
                                        )


class TestPipelineIntegration:
    """Test complete pipeline integration and workflow."""
    
    def test_pipeline_component_dependencies(self):
        """Test that pipeline properly initializes all component dependencies."""
        pipeline = NYCDataPipeline("/test/path")
        
        # Verify all components are properly initialized
        assert pipeline.loader.base_path == "/test/path"
        assert hasattr(pipeline, 'cleaner')
        assert hasattr(pipeline, 'transformer')
        assert hasattr(pipeline, 'integrator')
        
        # Verify components are of correct type
        assert isinstance(pipeline.loader, DataLoader)
        assert isinstance(pipeline.cleaner, DataCleaner)
        assert isinstance(pipeline.transformer, DataTransformer)
        assert isinstance(pipeline.integrator, DataIntegrator)
    
    def test_pipeline_step_sequence(self, pipeline_test_data, pipeline_intermediate_data):
        """Test that pipeline steps can be called in sequence."""
        pipeline = NYCDataPipeline()
        
        # Mock all external dependencies
        with patch.object(pipeline.loader, 'load_all_data') as mock_load:
            with patch.object(pipeline.loader, 'display_data_info'):
                with patch.object(pipeline.loader, 'get_relevant_columns') as mock_get_cols:
                    with patch.object(pipeline.cleaner, 'clean_nyc_311_data') as mock_clean_311:
                        with patch.object(pipeline.cleaner, 'clean_rent_data') as mock_clean_rent:
                            with patch.object(pipeline.transformer, 'transform_nyc_311_data') as mock_transform_311:
                                with patch.object(pipeline.transformer, 'transform_rent_data') as mock_transform_rent:
                                    with patch.object(pipeline.transformer, 'aggregate_complaints_by_neighborhood'):
                                        with patch.object(pipeline.transformer, 'aggregate_rent_by_neighborhood'):
                                            with patch.object(pipeline.transformer, 'reshape_rent_data'):
                                                with patch.object(pipeline.integrator, 'create_final_dataset') as mock_create:
                                                    with patch.object(pipeline.integrator, 'optimize_data_types') as mock_optimize:
                                                        with patch.object(pipeline.integrator, 'print_validation_results'):
                                                            with patch.object(pipeline.integrator, 'get_dataset_summary') as mock_summary:
                                                                with patch.object(pipeline.integrator, 'export_dataset'):
                                                                    
                                                                    # Setup returns
                                                                    mock_load.return_value = (
                                                                        pipeline_test_data['raw_nyc_311'],
                                                                        pipeline_test_data['raw_rent'],
                                                                        pipeline_test_data['uhf_mapping'],
                                                                        pipeline_test_data['manual_mapping']
                                                                    )
                                                                    mock_get_cols.return_value = ['complaint_type']
                                                                    mock_clean_311.return_value = pipeline_intermediate_data['cleaned_nyc_311']
                                                                    mock_clean_rent.return_value = pipeline_intermediate_data['cleaned_rent']
                                                                    mock_transform_311.return_value = pipeline_intermediate_data['transformed_nyc_311']
                                                                    mock_transform_rent.return_value = pipeline_intermediate_data['transformed_rent']
                                                                    mock_final = pd.DataFrame({'test': [1]})
                                                                    mock_create.return_value = mock_final
                                                                    mock_optimize.return_value = mock_final
                                                                    mock_summary.return_value = {}
                                                                    
                                                                    # Execute full pipeline sequence
                                                                    data = pipeline.load_data()
                                                                    cleaned = pipeline.clean_data(data[0], data[1])
                                                                    transformed = pipeline.transform_data(cleaned[0], cleaned[1], data[2], data[3])
                                                                    result = pipeline.aggregate_and_integrate(transformed[0], transformed[1])
                                                                    
                                                                    # Verify all steps executed successfully
                                                                    assert isinstance(result, pd.DataFrame)
    
    def test_pipeline_error_handling(self):
        """Test pipeline behavior when components raise errors."""
        pipeline = NYCDataPipeline()
        
        # Test error in load_data
        with patch.object(pipeline.loader, 'load_all_data', side_effect=Exception("Load error")):
            with pytest.raises(Exception, match="Load error"):
                pipeline.load_data()
        
        # Test error in clean_data
        with patch.object(pipeline.loader, 'get_relevant_columns', side_effect=Exception("Clean error")):
            with pytest.raises(Exception, match="Clean error"):
                pipeline.clean_data(pd.DataFrame(), pd.DataFrame())


class TestPipelineUtilityMethods:
    """Test utility methods and edge cases."""
    
    def test_pipeline_with_empty_data(self):
        """Test pipeline behavior with empty datasets."""
        pipeline = NYCDataPipeline()
        
        empty_df = pd.DataFrame()
        empty_dict = {}
        
        # Test clean_data with empty DataFrames
        with patch.object(pipeline.loader, 'get_relevant_columns', return_value=[]):
            with patch.object(pipeline.cleaner, 'clean_nyc_311_data', return_value=empty_df):
                with patch.object(pipeline.cleaner, 'clean_rent_data', return_value=empty_df):
                    result = pipeline.clean_data(empty_df, empty_df)
                    assert len(result) == 2
                    assert isinstance(result[0], pd.DataFrame)
                    assert isinstance(result[1], pd.DataFrame)
        
        # Test transform_data with empty DataFrames
        with patch.object(pipeline.transformer, 'transform_nyc_311_data', return_value=empty_df):
            with patch.object(pipeline.transformer, 'transform_rent_data', return_value=empty_df):
                result = pipeline.transform_data(empty_df, empty_df, empty_dict, empty_dict)
                assert len(result) == 2
                assert isinstance(result[0], pd.DataFrame)
                assert isinstance(result[1], pd.DataFrame)
    
    def test_pipeline_method_return_types(self, pipeline_test_data, pipeline_intermediate_data):
        """Test that all pipeline methods return correct data types."""
        pipeline = NYCDataPipeline()
        
        # Test load_data returns
        with patch.object(pipeline.loader, 'load_all_data') as mock_load:
            with patch.object(pipeline.loader, 'display_data_info'):
                mock_load.return_value = (
                    pipeline_test_data['raw_nyc_311'],
                    pipeline_test_data['raw_rent'],
                    pipeline_test_data['uhf_mapping'],
                    pipeline_test_data['manual_mapping']
                )
                result = pipeline.load_data()
                assert isinstance(result, tuple)
                assert len(result) == 4
                assert isinstance(result[0], pd.DataFrame)  # NYC 311 data
                assert isinstance(result[1], pd.DataFrame)  # Rent data
                assert isinstance(result[2], dict)          # UHF mapping
                assert isinstance(result[3], dict)          # Manual mapping
        
        # Test clean_data returns
        with patch.object(pipeline.loader, 'get_relevant_columns', return_value=['test']):
            with patch.object(pipeline.cleaner, 'clean_nyc_311_data', return_value=pipeline_intermediate_data['cleaned_nyc_311']):
                with patch.object(pipeline.cleaner, 'clean_rent_data', return_value=pipeline_intermediate_data['cleaned_rent']):
                    result = pipeline.clean_data(pd.DataFrame(), pd.DataFrame())
                    assert isinstance(result, tuple)
                    assert len(result) == 2
                    assert isinstance(result[0], pd.DataFrame)
                    assert isinstance(result[1], pd.DataFrame)
    
    def test_pipeline_stdout_output_structure(self, pipeline_test_data):
        """Test that pipeline methods produce properly formatted output."""
        pipeline = NYCDataPipeline()
        
        # Test load_data output format
        with patch.object(pipeline.loader, 'load_all_data') as mock_load:
            with patch.object(pipeline.loader, 'display_data_info'):
                mock_load.return_value = (pd.DataFrame(), pd.DataFrame(), {}, {})
                
                with patch('sys.stdout', new=StringIO()) as fake_out:
                    pipeline.load_data()
                
                output = fake_out.getvalue()
                assert "=" * 50 in output  # Verify separator formatting
                assert "STEP 1: LOADING DATA" in output
        
        # Test clean_data output format
        with patch.object(pipeline.loader, 'get_relevant_columns', return_value=[]):
            with patch.object(pipeline.cleaner, 'clean_nyc_311_data', return_value=pd.DataFrame()):
                with patch.object(pipeline.cleaner, 'clean_rent_data', return_value=pd.DataFrame()):
                    
                    with patch('sys.stdout', new=StringIO()) as fake_out:
                        pipeline.clean_data(pd.DataFrame(), pd.DataFrame())
                    
                    output = fake_out.getvalue()
                    assert "=" * 50 in output
                    assert "STEP 2: CLEANING DATA" in output