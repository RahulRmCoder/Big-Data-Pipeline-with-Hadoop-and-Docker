#!/usr/bin/env python3
"""
Data Preparation Script for Tableau/Power BI Visualization

This script prepares the Hive query results for visualization in Tableau or Power BI.
It combines all data sources into a single file and creates additional datasets
optimized for different visualization needs.
"""

import os
import pandas as pd
import json
from datetime import datetime


def prepare_data_for_visualization():
    """Prepare data for Tableau/Power BI visualization"""
    
    # Ensure output directories exist
    os.makedirs('data/visualization', exist_ok=True)
    
    # Define paths to the exported data
    exports_dir = 'data/exports/'
    web_traffic_path = os.path.join(exports_dir, 'web_traffic_by_endpoint')
    web_hourly_path = os.path.join(exports_dir, 'web_traffic_hourly')
    social_path = os.path.join(exports_dir, 'social_engagement')
    sensor_path = os.path.join(exports_dir, 'sensor_readings')
    correlation_path = os.path.join(exports_dir, 'correlation_data')
    
    # Function to load data from exported Hive query results
    def load_data(path, filename='000000_0'):
        # Check if the file exists
        full_path = os.path.join(path, filename)
        if not os.path.exists(full_path):
            print(f"Warning: File not found at {full_path}")
            # Try to find any file in the directory
            files = os.listdir(path)
            if files:
                full_path = os.path.join(path, files[0])
                print(f"Using file: {full_path}")
            else:
                print(f"No files found in {path}")
                return None
        
        # Load the data
        try:
            # Determine column names based on the path
            if 'web_traffic_by_endpoint' in path:
                cols = ['date', 'endpoint', 'total_requests', 'error_count', 'avg_response_time', 'unique_visitors']
            elif 'web_traffic_hourly' in path:
                cols = ['date', 'hour', 'total_requests', 'error_count', 'avg_response_time']
            elif 'social_engagement' in path:
                cols = ['date', 'platform', 'category', 'post_count', 'total_likes', 'total_shares', 
                       'total_comments', 'avg_engagement', 'avg_sentiment']
            elif 'sensor_readings' in path:
                cols = ['date', 'sensor_type', 'location', 'reading_count', 'avg_value', 'min_value', 
                       'max_value', 'active_readings', 'error_readings']
            elif 'correlation_data' in path:
                cols = ['date', 'total_requests', 'error_count', 'avg_response_time', 'post_count', 
                       'total_likes', 'total_shares', 'total_comments', 'avg_engagement', 'avg_sentiment']
            else:
                cols = None
                
            # Read the CSV file
            df = pd.read_csv(full_path, header=None, names=cols)
            
            # Convert date columns
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                
            return df
        
        except Exception as e:
            print(f"Error loading data: {e}")
            return None
    
    # Load all datasets
    web_traffic_df = load_data(web_traffic_path)
    web_hourly_df = load_data(web_hourly_path)
    social_df = load_data(social_path)
    sensor_df = load_data(sensor_path)
    correlation_df = load_data(correlation_path)
    
    # Create combined dataset for overall analysis
    datasets = {
        'web_traffic': web_traffic_df,
        'web_hourly': web_hourly_df,
        'social': social_df,
        'sensor': sensor_df,
        'correlation': correlation_df
    }
    
    # Process and save each dataset
    for name, df in datasets.items():
        if df is not None:
            # Clean and format the data
            if 'date' in df.columns:
                df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')
                df['year'] = df['date'].dt.year
                df['month'] = df['date'].dt.month
                df['day'] = df['date'].dt.day
                df['day_of_week'] = df['date'].dt.day_name()
            
            # Add dataset type column for combined dataset
            df['dataset_type'] = name
            
            # Save as CSV
            output_path = f'data/visualization/{name}_data.csv'
            df.to_csv(output_path, index=False)
            print(f"Saved {name} data to {output_path}")
    
    # Create consolidated file for Tableau/Power BI
    # This is a metadata file that describes all available datasets
    metadata = {
        'project_name': 'Big Data Pipeline with Hadoop and Hive',
        'generated_on': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'datasets': {}
    }
    
    for name, df in datasets.items():
        if df is not None:
            metadata['datasets'][name] = {
                'filename': f'{name}_data.csv',
                'record_count': len(df),
                'columns': df.columns.tolist(),
                'sample_path': f'data/visualization/{name}_data.csv'
            }
    
    # Save metadata as JSON
    with open('data/visualization/tableau_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print("Data preparation for Tableau/Power BI completed.")
    print("Files saved to data/visualization/ directory.")


if __name__ == "__main__":
    prepare_data_for_visualization()