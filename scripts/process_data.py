#!/usr/bin/env python3
"""
Data Processing Script for Hadoop-Hive Project

This script processes the collected data and uploads it to HDFS:
1. Processes web server logs
2. Processes social media data
3. Processes sensor data
4. Uploads processed data to HDFS
"""

import os
import csv
import json
import subprocess
import pandas as pd
from datetime import datetime

def process_web_logs():
    """Process web server logs and prepare for HDFS"""
    
    input_file = 'data/raw/logs/web_access_logs.csv'
    output_file = 'data/processed/web_logs_processed.csv'
    
    # Ensure output directory exists
    os.makedirs('data/processed', exist_ok=True)
    
    print(f"Processing web logs from {input_file}...")
    
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract date and time components
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        
        # Fill missing user_id
        df['user_id'] = df['user_id'].fillna('anonymous')
        
        # Create success/error column
        df['is_error'] = df['status_code'] >= 400
        
        # Save processed data
        df.to_csv(output_file, index=False)
        
        print(f"Processed web logs saved to {output_file}")
        return True
    
    except Exception as e:
        print(f"Error processing web logs: {e}")
        return False

def process_social_data():
    """Process social media data and prepare for HDFS"""
    
    input_file = 'data/raw/social/social_data.json'
    output_file = 'data/processed/social_data_processed.csv'
    
    print(f"Processing social data from {input_file}...")
    
    try:
        # Read the JSON file
        with open(input_file, 'r') as f:
            social_data = json.load(f)
        
        # Convert to DataFrame
        df = pd.DataFrame(social_data)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract date and time components
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        
        # Create engagement score
        df['engagement_score'] = df['likes'] + (df['shares'] * 2) + (df['comments'] * 3)
        
        # Create sentiment score
        sentiment_map = {'positive': 1, 'neutral': 0, 'negative': -1}
        df['sentiment_score'] = df['sentiment'].map(sentiment_map)
        
        # Save processed data
        df.to_csv(output_file, index=False)
        
        print(f"Processed social data saved to {output_file}")
        return True
    
    except Exception as e:
        print(f"Error processing social data: {e}")
        return False

def process_sensor_data():
    """Process sensor data and prepare for HDFS"""
    
    input_file = 'data/raw/logs/sensor_data.csv'
    output_file = 'data/processed/sensor_data_processed.csv'
    
    print(f"Processing sensor data from {input_file}...")
    
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract date and time components
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        
        # Create status flag
        df['is_active'] = df['status'] == 'active'
        
        # Create battery level category
        df['battery_category'] = pd.cut(
            df['battery_level'], 
            bins=[0, 20, 50, 80, 100], 
            labels=['critical', 'low', 'medium', 'high']
        )
        
        # Save processed data
        df.to_csv(output_file, index=False)
        
        print(f"Processed sensor data saved to {output_file}")
        return True
    
    except Exception as e:
        print(f"Error processing sensor data: {e}")
        return False

def upload_to_hdfs():
    """Upload processed data to HDFS"""
    
    # List of files to upload
    files = [
        'data/processed/web_logs_processed.csv',
        'data/processed/social_data_processed.csv',
        'data/processed/sensor_data_processed.csv'
    ]
    
    # HDFS destination directory
    hdfs_dir = '/user/hive/warehouse/raw_data'
    
    print(f"Uploading processed data to HDFS directory: {hdfs_dir}")
    
    # Execute commands to upload files
    try:
        # Run the upload in Docker container
        for file in files:
            file_name = os.path.basename(file)
            hdfs_path = f"{hdfs_dir}/{file_name}"
            
            # Create destination directory
            cmd = f"docker exec -it namenode hdfs dfs -mkdir -p {hdfs_dir}"
            subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Copy file from local to container
            cmd = f"docker cp {file} namenode:/tmp/{file_name}"
            subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Copy file from container to HDFS
            cmd = f"docker exec -it namenode hdfs dfs -put -f /tmp/{file_name} {hdfs_path}"
            subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            print(f"Uploaded {file} to HDFS at {hdfs_path}")
        
        print("All files uploaded to HDFS successfully")
        return True
    
    except subprocess.CalledProcessError as e:
        print(f"Error uploading to HDFS: {e}")
        print(f"Command output: {e.stderr.decode()}")
        return False

if __name__ == "__main__":
    print("Starting data processing...")
    
    # Process data
    web_logs_ok = process_web_logs()
    social_data_ok = process_social_data()
    sensor_data_ok = process_sensor_data()
    
    # Upload to HDFS if all processing steps succeeded
    if web_logs_ok and social_data_ok and sensor_data_ok:
        upload_to_hdfs()
    else:
        print("Skipping HDFS upload due to processing errors")
    
    print("Data processing completed.")