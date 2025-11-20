"""
Data Integration Module

This module handles merging and integration of different datasets.
"""

import pandas as pd
from typing import List


class DataIntegrator:
    """Handles data integration and merging operations."""
    
    def __init__(self):
        """Initialize DataIntegrator."""
        pass
    
    def merge_complaints_and_rent(self, complaints_df: pd.DataFrame, 
                                 rent_df: pd.DataFrame,
                                 merge_columns: List[str] = None,
                                 how: str = 'left') -> pd.DataFrame:
        """
        Merge complaints and rent data.
        
        Args:
            complaints_df (pd.DataFrame): Aggregated complaints data
            rent_df (pd.DataFrame): Reshaped rent data
            merge_columns (List[str]): Columns to merge on
            how (str): Type of merge to perform
            
        Returns:
            pd.DataFrame: Merged dataset
        """
        if merge_columns is None:
            merge_columns = ['neighborhood', 'year', 'month']
        
        # Select relevant columns from rent data for merging
        rent_columns_for_merge = merge_columns + ['median_rent']
        
        df_merged = pd.merge(
            complaints_df, 
            rent_df[rent_columns_for_merge], 
            on=merge_columns, 
            how=how
        )

        print(f"Final merged dataset shape: {df_merged.shape}")
        print(f"Records with rent data: {df_merged['median_rent'].notna().sum()}")
        print(f"Records without rent data: {df_merged['median_rent'].isna().sum()}")
        
        return df_merged
    
    def create_final_dataset(self, complaints_df: pd.DataFrame, 
                           rent_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create final integrated dataset from complaints and rent data.
        
        Args:
            complaints_df (pd.DataFrame): Aggregated complaints data
            rent_df (pd.DataFrame): Reshaped rent data
            
        Returns:
            pd.DataFrame: Final integrated dataset
        """
        print("Creating final integrated dataset...")
        
        # Merge the datasets
        df_final = self.merge_complaints_and_rent(complaints_df, rent_df)
        
        print("Final dataset integration completed!")
        return df_final
    
    def export_dataset(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Export dataset to CSV file.
        
        Args:
            df (pd.DataFrame): DataFrame to export
            output_path (str): Path for output file
        """
        df.to_csv(output_path, index=False)
        print(f"Dataset exported to: {output_path}")
        print(f"Dataset shape: {df.shape}")
    
    def get_dataset_summary(self, df: pd.DataFrame) -> dict:
        """
        Get summary statistics for the final dataset.
        
        Args:
            df (pd.DataFrame): Final dataset
            
        Returns:
            dict: Summary statistics
        """
        summary = {
            'total_records': len(df),
            'date_range': f"{df['year'].min()}-{df['year'].max()}",
            'unique_neighborhoods': df['neighborhood'].nunique(),
            'unique_complaint_types': df['complaint_type'].nunique(),
            'data_completeness': (df.notna().sum() / len(df) * 100).round(2).to_dict()
        }
        
        return summary
    
    def print_dataset_summary(self, df: pd.DataFrame) -> None:
        """
        Print comprehensive dataset summary.
        
        Args:
            df (pd.DataFrame): Final dataset
        """
        summary = self.get_dataset_summary(df)
        
        print("=== Final Dataset Summary ===")
        print(f"Total records: {summary['total_records']:,}")
        print(f"Date range: {summary['date_range']}")
        print(f"Unique neighborhoods: {summary['unique_neighborhoods']}")
        print(f"Unique complaint types: {summary['unique_complaint_types']}")

        print("\n=== Data Completeness ===")
        for column, completeness in summary['data_completeness'].items():
            print(f"{column}: {completeness}%")

        print("\n=== Top 10 Neighborhoods by Complaint Volume ===")
        top_neighborhoods = df.groupby('neighborhood')['complaint_count'].sum().sort_values(ascending=False).head(10)
        for neighborhood, count in top_neighborhoods.items():
            print(f"{neighborhood}: {count:,}")
    
    def validate_merged_data(self, df: pd.DataFrame) -> dict:
        """
        Validate the quality of merged data.
        
        Args:
            df (pd.DataFrame): Merged dataset
            
        Returns:
            dict: Validation results
        """
        validation_results = {
            'has_duplicates': df.duplicated().any(),
            'missing_neighborhoods': df['neighborhood'].isna().sum(),
            'missing_rent_data': df['median_rent'].isna().sum(),
            'invalid_years': (df['year'] < 2024).sum() + (df['year'] > 2025).sum(),
            'invalid_months': (df['month'] < 1).sum() + (df['month'] > 12).sum(),
            'negative_complaints': (df['complaint_count'] < 0).sum(),
            'negative_resolution_time': (df['median_resolution_time_hours'] < 0).sum()
        }
        
        return validation_results
    
    def print_validation_results(self, df: pd.DataFrame) -> None:
        """
        Print data validation results.
        
        Args:
            df (pd.DataFrame): Dataset to validate
        """
        results = self.validate_merged_data(df)
        
        print("=== Data Validation Results ===")
        for check, value in results.items():
            status = "✓ PASS" if value == 0 else f"✗ FAIL ({value})"
            print(f"{check.replace('_', ' ').title()}: {status}")
    
    def get_sample_data(self, df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """
        Get sample of merged data for inspection.
        
        Args:
            df (pd.DataFrame): Dataset
            n (int): Number of samples to return
            
        Returns:
            pd.DataFrame: Sample data
        """
        return df.head(n)
    
    def optimize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize data types for memory efficiency and performance using intelligent analysis.
        
        Args:
            df (pd.DataFrame): Dataset to optimize
            
        Returns:
            pd.DataFrame: Dataset with optimized data types
        """
        print("=== OPTIMIZING DATA TYPES ===")
        
        # Create optimized copy
        optimized_df = df.copy()
        original_memory = df.memory_usage(deep=True).sum() / 1024**2
        
        # 1. Automatically detect and convert categorical columns
        print("📊 Analyzing and converting categorical columns...")
        categorical_columns = self._detect_categorical_columns(df)
        for col in categorical_columns:
            print(f"  Converting '{col}' to category ({df[col].nunique()} unique values)")
            optimized_df[col] = optimized_df[col].astype('category')
        
        # 2. Optimize numeric types based on actual ranges
        print("🔢 Analyzing and optimizing numeric columns...")
        numeric_optimizations = self._optimize_numeric_columns(optimized_df)
        
        for col, (original_type, new_type, reason) in numeric_optimizations.items():
            print(f"  '{col}': {original_type} → {new_type} ({reason})")
            optimized_df[col] = optimized_df[col].astype(new_type)
        
        print("✅ Optimization complete!")
        
        # Calculate memory savings
        optimized_memory = optimized_df.memory_usage(deep=True).sum() / 1024**2
        savings = ((original_memory - optimized_memory) / original_memory) * 100
        
        print(f"\n📊 MEMORY OPTIMIZATION RESULTS:")
        print(f"Original:  {original_memory:.2f} MB")
        print(f"Optimized: {optimized_memory:.2f} MB")
        print(f"Savings:   {savings:.1f}% ({original_memory - optimized_memory:.2f} MB)")
        
        return optimized_df
    
    def _detect_categorical_columns(self, df: pd.DataFrame, categorical_threshold: float = 0.05) -> List[str]:
        """
        Automatically detect categorical columns based on uniqueness ratio.
        
        Args:
            df (pd.DataFrame): Dataset to analyze
            categorical_threshold (float): Max ratio of unique values to total rows for categorical detection
            
        Returns:
            List[str]: List of column names that should be categorical
        """
        categorical_columns = []
        
        for col in df.columns:
            # Skip already categorical columns
            if df[col].dtype.name == 'category':
                continue
                
            # Only consider object (string) columns
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df)
                
                # If unique ratio is low, it's likely categorical
                if unique_ratio <= categorical_threshold:
                    categorical_columns.append(col)
        
        return categorical_columns
    
    def _optimize_numeric_columns(self, df: pd.DataFrame) -> dict:
        """
        Automatically determine optimal numeric types based on value ranges.
        
        Args:
            df (pd.DataFrame): Dataset to analyze
            
        Returns:
            dict: Dictionary mapping column names to (original_type, new_type, reason)
        """
        optimizations = {}
        
        for col in df.columns:
            # Skip non-numeric columns
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            # Skip already optimized small types
            if df[col].dtype in ['uint8', 'int8', 'uint16', 'int16', 'float32']:
                continue
            
            original_type = str(df[col].dtype)
            
            # Handle integer columns
            if df[col].dtype in ['int64', 'int32'] or df[col].dtype.name.startswith('int'):
                new_type, reason = self._optimize_integer_column(df[col])
                if new_type != original_type:
                    optimizations[col] = (original_type, new_type, reason)
            
            # Handle float columns
            elif df[col].dtype in ['float64'] or df[col].dtype.name.startswith('float'):
                new_type, reason = self._optimize_float_column(df[col])
                if new_type != original_type:
                    optimizations[col] = (original_type, new_type, reason)
        
        return optimizations
    
    def _optimize_integer_column(self, series: pd.Series) -> tuple:
        """
        Determine optimal integer type for a column.
        
        Args:
            series (pd.Series): Integer column to optimize
            
        Returns:
            tuple: (optimal_type, reason)
        """
        if series.isna().any():
            # Has nulls, can't use unsigned types
            min_val, max_val = series.min(), series.max()
            if -128 <= min_val and max_val <= 127:
                return 'int8', f'range [{min_val}, {max_val}] fits in int8'
            elif -32768 <= min_val and max_val <= 32767:
                return 'int16', f'range [{min_val}, {max_val}] fits in int16'
            else:
                return 'int32', f'range [{min_val}, {max_val}] requires int32'
        else:
            # No nulls, can consider unsigned types
            min_val, max_val = series.min(), series.max()
            
            if min_val >= 0:
                # All positive values
                if max_val <= 255:
                    return 'uint8', f'range [0, {max_val}] fits in uint8'
                elif max_val <= 65535:
                    return 'uint16', f'range [0, {max_val}] fits in uint16'
                else:
                    return 'uint32', f'range [0, {max_val}] requires uint32'
            else:
                # Has negative values
                if -128 <= min_val and max_val <= 127:
                    return 'int8', f'range [{min_val}, {max_val}] fits in int8'
                elif -32768 <= min_val and max_val <= 32767:
                    return 'int16', f'range [{min_val}, {max_val}] fits in int16'
                else:
                    return 'int32', f'range [{min_val}, {max_val}] requires int32'
    
    def _optimize_float_column(self, series: pd.Series) -> tuple:
        """
        Determine optimal float type for a column.
        
        Args:
            series (pd.Series): Float column to optimize
            
        Returns:
            tuple: (optimal_type, reason)
        """
        # For most use cases, float32 provides sufficient precision
        # and uses half the memory of float64
        return 'float32', 'sufficient precision, 50% memory reduction'
    