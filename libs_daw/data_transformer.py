"""
Data Transformation Module

This module handles feature engineering, geographic mapping, and data aggregation.
"""

import pandas as pd
from typing import Dict, Any, List, Tuple


class DataTransformer:
    """Handles data transformation and feature engineering operations."""
    
    def __init__(self):
        """Initialize DataTransformer."""
        pass
    
    def create_time_features(self, df: pd.DataFrame, date_column: str = 'created_date') -> pd.DataFrame:
        """
        Create time-based features from date column.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            date_column (str): Name of date column to extract features from
            
        Returns:
            pd.DataFrame: DataFrame with new time features
        """
        df_transformed = df.copy()
        
        if date_column in df_transformed.columns:
            # Extract month and year
            df_transformed['month'] = df_transformed[date_column].dt.month
            df_transformed['year'] = df_transformed[date_column].dt.year
            
            print(f"Created time features: month, year from {date_column}")
        
        return df_transformed
    
    def calculate_resolution_time(self, df: pd.DataFrame, 
                                created_col: str = 'created_date',
                                closed_col: str = 'closed_date') -> pd.DataFrame:
        """
        Calculate resolution time in hours.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            created_col (str): Name of creation date column
            closed_col (str): Name of closure date column
            
        Returns:
            pd.DataFrame: DataFrame with resolution time feature
        """
        df_transformed = df.copy()
        
        if created_col in df_transformed.columns and closed_col in df_transformed.columns:
            df_transformed['resolution_time_hours'] = (
                df_transformed[closed_col] - df_transformed[created_col]
            ).dt.total_seconds() / 3600
            
            print("Created feature: resolution_time_hours")
            print("Resolution time statistics:")
            print(df_transformed['resolution_time_hours'].describe())
        
        return df_transformed
    
    def create_zip_to_neighborhood_mapping(self, uhf_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Create ZIP code to neighborhood mapping dictionary.
        
        Args:
            uhf_data (Dict): UHF data containing borough and neighborhood information
            
        Returns:
            Dict[str, str]: ZIP code to neighborhood mapping
        """
        zip_to_neighborhood = {}

        for borough, neighborhoods in uhf_data.items():
            for neighborhood_info in neighborhoods:
                neighborhood_name = neighborhood_info['neighborhood']
                zip_codes = neighborhood_info['zip_codes']
                
                for zip_code in zip_codes:
                    zip_to_neighborhood[zip_code] = neighborhood_name

        print(f"Created mapping for {len(zip_to_neighborhood)} ZIP codes to neighborhoods")
        return zip_to_neighborhood
    
    def create_neighborhood_to_borough_mapping(self, uhf_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Create neighborhood to borough mapping dictionary from UHF data.
        
        Args:
            uhf_data (Dict): UHF data containing borough and neighborhood information
            
        Returns:
            Dict[str, str]: Neighborhood to borough mapping
        """
        neighborhood_to_borough = {}

        for borough, neighborhoods in uhf_data.items():
            for neighborhood_info in neighborhoods:
                neighborhood_name = neighborhood_info['neighborhood']
                neighborhood_to_borough[neighborhood_name] = borough

        print(f"Created borough mapping for {len(neighborhood_to_borough)} neighborhoods")
        return neighborhood_to_borough
    
    def apply_borough_mapping(self, df: pd.DataFrame, 
                             neighborhood_to_borough: Dict[str, str],
                             neighborhood_column: str = 'neighborhood') -> pd.DataFrame:
        """
        Apply borough mapping to neighborhoods based on UHF data.
        
        Args:
            df (pd.DataFrame): DataFrame with neighborhood column
            neighborhood_to_borough (Dict): Neighborhood to borough mapping from UHF data
            neighborhood_column (str): Name of neighborhood column
            
        Returns:
            pd.DataFrame: DataFrame with corrected borough assignments
        """
        df_mapped = df.copy()
        
        if neighborhood_column in df_mapped.columns:
            # Create a new borough column based on UHF mapping
            df_mapped['borough_from_uhf'] = df_mapped[neighborhood_column].map(neighborhood_to_borough)
            
            # Use UHF borough where available, fallback to original borough
            df_mapped['borough'] = df_mapped['borough_from_uhf'].fillna(df_mapped.get('borough', ''))
            
            # Clean up temporary column
            df_mapped = df_mapped.drop('borough_from_uhf', axis=1)
            
            # Report mapping results
            mapped_count = df_mapped['borough'].notna().sum()
            total_count = len(df_mapped)
            
            print(f"Borough mapping results:")
            print(f"Records with borough assigned: {mapped_count}")
            print(f"Records without borough: {total_count - mapped_count}")
            
            # Show borough distribution
            if mapped_count > 0:
                print("Borough distribution:")
                print(df_mapped['borough'].value_counts().to_dict())
        
        return df_mapped
    
    def map_zip_to_neighborhood(self, df: pd.DataFrame, 
                               zip_to_neighborhood: Dict[str, str],
                               zip_column: str = 'incident_zip') -> pd.DataFrame:
        """
        Map ZIP codes to neighborhoods.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            zip_to_neighborhood (Dict): ZIP to neighborhood mapping
            zip_column (str): Name of ZIP code column
            
        Returns:
            pd.DataFrame: DataFrame with neighborhood column
        """
        df_mapped = df.copy()
        
        if zip_column in df_mapped.columns:
            # Convert ZIP codes to string format, handling various data types
            def safe_zip_conversion(x):
                if pd.isna(x):
                    return None
                try:
                    # Try to convert to int first, then to string with zero padding
                    return str(int(float(x))).zfill(5)
                except (ValueError, TypeError):
                    # If conversion fails, try to use as string directly
                    try:
                        return str(x).zfill(5) if str(x).isdigit() else None
                    except:
                        return None
            
            df_mapped['incident_zip_str'] = df_mapped[zip_column].apply(safe_zip_conversion)

            # Map to neighborhoods
            df_mapped['neighborhood'] = df_mapped['incident_zip_str'].map(zip_to_neighborhood)

            # Report mapping results
            mapped_records = df_mapped['neighborhood'].notna().sum()
            total_records = len(df_mapped)
            coverage_percentage = (mapped_records / total_records * 100)

            print(f"Neighborhood mapping results:")
            print(f"Records with neighborhood: {mapped_records:,}")
            print(f"Records without neighborhood: {total_records - mapped_records:,}")
            print(f"Coverage percentage: {coverage_percentage:.2f}%")

            # Clean up temporary column
            df_mapped = df_mapped.drop('incident_zip_str', axis=1)
        
        return df_mapped
    
    def map_area_to_neighborhood(self, df: pd.DataFrame, 
                                manual_map: Dict[str, str],
                                area_column: str = 'areaName') -> pd.DataFrame:
        """
        Map area names to neighborhoods using manual mapping.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            manual_map (Dict): Manual area to neighborhood mapping
            area_column (str): Name of area column
            
        Returns:
            pd.DataFrame: DataFrame with neighborhood column
        """
        df_mapped = df.copy()
        
        if area_column in df_mapped.columns:
            df_mapped['neighborhood'] = df_mapped[area_column].str.lower().map(manual_map)

            # Report mapping results
            mapped_records = df_mapped['neighborhood'].notna().sum()
            total_records = len(df_mapped)

            print(f"Area to neighborhood mapping results:")
            print(f"Records with neighborhood: {mapped_records}")
            print(f"Records without neighborhood: {total_records - mapped_records}")
            print(f"Coverage percentage: {(mapped_records / total_records * 100):.2f}%")
        
        return df_mapped
    
    def aggregate_complaints_by_neighborhood(self, df: pd.DataFrame, 
                                          group_columns: List[str] = None) -> pd.DataFrame:
        """
        Aggregate complaints by neighborhood, complaint type, year, and month.
        
        Args:
            df (pd.DataFrame): Input DataFrame with complaints
            group_columns (List[str]): Columns to group by
            
        Returns:
            pd.DataFrame: Aggregated complaints data
        """
        if group_columns is None:
            group_columns = ['neighborhood', 'borough', 'complaint_type', 'year', 'month']
        
        complaints_aggregated = df.groupby(group_columns).agg({
            'resolution_time_hours': ['count', 'median']
        }).reset_index()

        # Flatten column names
        complaints_aggregated.columns = (
            group_columns + ['complaint_count', 'median_resolution_time_hours']
        )

        # Sort by neighborhood, year, month, and complaint count
        complaints_aggregated = complaints_aggregated.sort_values(
            by=['borough', 'neighborhood', 'year', 'month', 'complaint_count'], 
            ascending=[True, True, True, True, False]
        )

        print(f"Aggregated complaints data shape: {complaints_aggregated.shape}")
        print(f"Unique neighborhoods in complaints: {complaints_aggregated['neighborhood'].nunique()}")
        
        return complaints_aggregated
    
    def aggregate_rent_by_neighborhood(self, df: pd.DataFrame, 
                                     years: List[str] = None) -> pd.DataFrame:
        """
        Aggregate rent data by neighborhood.
        
        Args:
            df (pd.DataFrame): Input DataFrame with rent data
            years (List[str]): Years to include in aggregation
            
        Returns:
            pd.DataFrame: Aggregated rent data
        """
        if years is None:
            years = ['2024', '2025']
        
        date_columns = [col for col in df.columns 
                       if any(col.startswith(year) for year in years)]
        
        median_rent_by_neighborhood = df.groupby('neighborhood')[date_columns].median()

        print(f"Aggregated rent data shape: {median_rent_by_neighborhood.shape}")
        print(f"Unique neighborhoods in rent data: {median_rent_by_neighborhood.index.nunique()}")
        
        return median_rent_by_neighborhood
    
    def reshape_rent_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reshape rent data from wide to long format.
        
        Args:
            df (pd.DataFrame): Wide format rent data
            
        Returns:
            pd.DataFrame: Long format rent data
        """
        # Reshape from wide to long format
        rent_melted = df.reset_index().melt(
            id_vars='neighborhood', 
            var_name='date', 
            value_name='median_rent'
        )

        # Convert date column and extract year/month
        rent_melted['date'] = pd.to_datetime(rent_melted['date'])
        rent_melted['year'] = rent_melted['date'].dt.year
        rent_melted['month'] = rent_melted['date'].dt.month

        print(f"Reshaped rent data shape: {rent_melted.shape}")
        return rent_melted
    
    def transform_nyc_311_data(self, df: pd.DataFrame, uhf_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Complete transformation pipeline for NYC 311 data.
        
        Args:
            df (pd.DataFrame): Cleaned NYC 311 DataFrame
            uhf_data (Dict): UHF mapping data
            
        Returns:
            pd.DataFrame: Transformed NYC 311 DataFrame
        """
        print("Starting NYC 311 data transformation pipeline...")
        
        # Calculate resolution time
        df_transformed = self.calculate_resolution_time(df)
        
        # Create time features
        df_transformed = self.create_time_features(df_transformed)
        
        # Create ZIP to neighborhood mapping
        zip_to_neighborhood = self.create_zip_to_neighborhood_mapping(uhf_data)
        
        # Map ZIP codes to neighborhoods
        df_transformed = self.map_zip_to_neighborhood(df_transformed, zip_to_neighborhood)
        
        # Create neighborhood to borough mapping from UHF data
        neighborhood_to_borough = self.create_neighborhood_to_borough_mapping(uhf_data)
        
        # Apply correct borough mapping based on UHF data
        df_transformed = self.apply_borough_mapping(df_transformed, neighborhood_to_borough)
        
        print("NYC 311 data transformation completed!")
        return df_transformed
    
    def transform_rent_data(self, df: pd.DataFrame, manual_map: Dict[str, str], uhf_data: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Complete transformation pipeline for rent data.
        
        Args:
            df (pd.DataFrame): Cleaned rent DataFrame
            manual_map (Dict): Manual area to neighborhood mapping
            uhf_data (Dict): UHF mapping data for borough assignment
            
        Returns:
            pd.DataFrame: Transformed rent DataFrame
        """
        print("Starting rent data transformation pipeline...")
        
        # Map areas to neighborhoods
        df_transformed = self.map_area_to_neighborhood(df, manual_map)
        
        # If UHF data is provided, apply borough mapping
        if uhf_data is not None:
            neighborhood_to_borough = self.create_neighborhood_to_borough_mapping(uhf_data)
            df_transformed = self.apply_borough_mapping(df_transformed, neighborhood_to_borough)
        
        print("Rent data transformation completed!")
        return df_transformed