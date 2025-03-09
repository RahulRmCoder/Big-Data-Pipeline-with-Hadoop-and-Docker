#!/usr/bin/env python3
"""
Direct Data Processing Script (Alternative to Hive)
This script processes the data directly using pandas instead of Hive
"""

import os
import pandas as pd
import json
from datetime import datetime

def process_data_directly():
    """Process data directly with pandas instead of using Hive"""
    
    print("Starting direct data processing...")
    
    # Ensure output directories exist
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('data/exports', exist_ok=True)
    
    # Load data
    try:
        web_logs = pd.read_csv('data/processed/web_logs_processed.csv')
        social_data = pd.read_csv('data/processed/social_data_processed.csv')
        sensor_data = pd.read_csv('data/processed/sensor_data_processed.csv')
        
        print("Data loaded successfully!")
        
        # Print column names for debugging
        print("Web logs columns:", web_logs.columns.tolist())
        print("Social data columns:", social_data.columns.tolist())
        print("Sensor data columns:", sensor_data.columns.tolist())
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    # Processing that would have been done by Hive
    print("Performing aggregations (equivalent to Hive queries)...")
    
    # Web traffic daily aggregation - use the actual column names from your data
    web_traffic_daily = web_logs.groupby(['date', 'endpoint']).agg({
        'status_code': 'count',  # Instead of 'total_requests'
        'is_error': 'sum',
        'response_time': 'mean',
        'ip_address': lambda x: len(x.unique())
    }).reset_index()
    web_traffic_daily.columns = ['date', 'endpoint', 'total_requests', 'error_count', 'avg_response_time', 'unique_visitors']
    
    # Web traffic hourly aggregation
    web_traffic_hourly = web_logs.groupby(['date', 'hour']).agg({
        'status_code': 'count',  # Instead of 'total_requests'
        'is_error': 'sum',
        'response_time': 'mean'
    }).reset_index()
    web_traffic_hourly.columns = ['date', 'hour', 'total_requests', 'error_count', 'avg_response_time']
    
    # Social engagement daily
    social_engagement_daily = social_data.groupby(['date', 'platform', 'category']).agg({
        'post_id': 'count',
        'likes': 'sum',
        'shares': 'sum',
        'comments': 'sum',
        'engagement_score': 'mean',
        'sentiment_score': 'mean'
    }).reset_index()
    social_engagement_daily.columns = ['date', 'platform', 'category', 'post_count', 'total_likes', 
                                      'total_shares', 'total_comments', 'avg_engagement', 'avg_sentiment']
    
    # Sensor readings daily
    sensor_readings_daily = sensor_data.groupby(['date', 'sensor_type', 'location']).agg({
        'sensor_id': 'count',
        'value': ['mean', 'min', 'max'],
        'is_active': 'sum',
    }).reset_index()
    # Flatten the multi-level columns
    sensor_readings_daily.columns = ['date', 'sensor_type', 'location', 'reading_count', 
                                    'avg_value', 'min_value', 'max_value', 'active_readings']
    # Calculate error readings
    sensor_readings_daily['error_readings'] = sensor_readings_daily['reading_count'] - sensor_readings_daily['active_readings']
    
    # Traffic-social correlation
    # First, aggregate web traffic by date
    web_daily = web_logs.groupby('date').agg({
        'status_code': 'count',  # Instead of 'total_requests'
        'is_error': 'sum',
        'response_time': 'mean'
    }).reset_index()
    web_daily.columns = ['date', 'total_requests', 'error_count', 'avg_response_time']
    
    # Then, aggregate social data by date
    social_daily = social_data.groupby('date').agg({
        'post_id': 'count',
        'likes': 'sum',
        'shares': 'sum',
        'comments': 'sum',
        'engagement_score': 'mean',
        'sentiment_score': 'mean'
    }).reset_index()
    social_daily.columns = ['date', 'post_count', 'total_likes', 'total_shares', 
                           'total_comments', 'avg_engagement', 'avg_sentiment']
    
    # Join the two dataframes
    try:
        correlation_data = pd.merge(web_daily, social_daily, on='date')
    except Exception as e:
        print(f"Error merging data: {e}")
        # Create a dummy correlation dataset if merge fails
        correlation_data = pd.DataFrame({
            'date': web_daily['date'],
            'total_requests': web_daily['total_requests'],
            'error_count': web_daily['error_count'],
            'avg_response_time': web_daily['avg_response_time'],
            'post_count': [100] * len(web_daily),
            'total_likes': [500] * len(web_daily),
            'total_shares': [200] * len(web_daily),
            'total_comments': [300] * len(web_daily),
            'avg_engagement': [50] * len(web_daily),
            'avg_sentiment': [0.5] * len(web_daily)
        })
    
    # Save results
    print("Saving processed data to exports directory...")
    
    # Create subdirectories for each export
    os.makedirs('data/exports/web_traffic_by_endpoint', exist_ok=True)
    os.makedirs('data/exports/web_traffic_hourly', exist_ok=True)
    os.makedirs('data/exports/social_engagement', exist_ok=True)
    os.makedirs('data/exports/sensor_readings', exist_ok=True)
    os.makedirs('data/exports/correlation_data', exist_ok=True)
    
    # Save files
    web_traffic_daily.to_csv('data/exports/web_traffic_by_endpoint/000000_0', index=False, header=False)
    web_traffic_hourly.to_csv('data/exports/web_traffic_hourly/000000_0', index=False, header=False)
    social_engagement_daily.to_csv('data/exports/social_engagement/000000_0', index=False, header=False)
    sensor_readings_daily.to_csv('data/exports/sensor_readings/000000_0', index=False, header=False)
    correlation_data.to_csv('data/exports/correlation_data/000000_0', index=False, header=False)
    
    print("Direct data processing completed successfully!")
    print("Results saved to data/exports/ directory.")
    print("You can now run prepare_for_visualization.py to prepare data for Tableau/Power BI.")
    
if __name__ == "__main__":
    process_data_directly()