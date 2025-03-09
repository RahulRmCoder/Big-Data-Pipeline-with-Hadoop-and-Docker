#!/usr/bin/env python3
"""
Data Collection Script for Hadoop-Hive Project

This script collects data from various sources:
1. Generates synthetic web server logs
2. Creates synthetic social media data
"""

import os
import csv
import json
import random
import datetime
import time
from faker import Faker

# Initialize Faker for generating synthetic data
fake = Faker()

# Create output directories if they don't exist
os.makedirs('data/raw/logs', exist_ok=True)
os.makedirs('data/raw/social', exist_ok=True)

def generate_web_logs(num_entries=1000):
    """Generate synthetic web server logs"""
    
    log_file = 'data/raw/logs/web_access_logs.csv'
    
    # HTTP methods and status codes for random generation
    http_methods = ['GET', 'POST', 'PUT', 'DELETE']
    status_codes = [200, 201, 204, 400, 403, 404, 500]
    pages = ['/home', '/about', '/contact', '/products', '/services', '/blog', '/login', '/register']
    
    with open(log_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'ip_address', 'user_id', 'method', 'endpoint', 'status_code', 'response_time', 'user_agent'])
        
        for _ in range(num_entries):
            timestamp = fake.date_time_between(start_date='-7d', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
            ip_address = fake.ipv4()
            user_id = random.choice([None, fake.uuid4()]) if random.random() > 0.3 else None
            method = random.choice(http_methods)
            endpoint = random.choice(pages)
            status_code = random.choice(status_codes)
            response_time = round(random.uniform(0.1, 2.0), 3)
            user_agent = fake.user_agent()
            
            writer.writerow([timestamp, ip_address, user_id, method, endpoint, status_code, response_time, user_agent])
    
    print(f"Generated {num_entries} web log entries in {log_file}")

def generate_social_data(num_entries=500):
    """Generate synthetic social media data"""
    
    social_file = 'data/raw/social/social_data.json'
    
    # Sentiment and categories for random generation
    sentiments = ['positive', 'neutral', 'negative']
    categories = ['product', 'service', 'support', 'general']
    
    social_data = []
    
    for _ in range(num_entries):
        post = {
            'post_id': fake.uuid4(),
            'user_handle': fake.user_name(),
            'timestamp': fake.date_time_between(start_date='-7d', end_date='now').isoformat(),
            'content': fake.text(max_nb_chars=280),
            'likes': random.randint(0, 1000),
            'shares': random.randint(0, 200),
            'comments': random.randint(0, 50),
            'sentiment': random.choice(sentiments),
            'category': random.choice(categories),
            'platform': random.choice(['twitter', 'facebook', 'instagram', 'linkedin'])
        }
        social_data.append(post)
    
    with open(social_file, 'w') as f:
        json.dump(social_data, f, indent=2)
    
    print(f"Generated {num_entries} social media entries in {social_file}")

def generate_sensor_data(num_entries=2000):
    """Generate synthetic IoT sensor data"""
    
    sensor_file = 'data/raw/logs/sensor_data.csv'
    
    # Sensor types and locations
    sensor_types = ['temperature', 'humidity', 'pressure', 'light', 'co2']
    locations = ['room1', 'room2', 'room3', 'outside', 'basement']
    
    with open(sensor_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'sensor_id', 'sensor_type', 'location', 'value', 'battery_level', 'status'])
        
        for _ in range(num_entries):
            timestamp = fake.date_time_between(start_date='-7d', end_date='now').strftime('%Y-%m-%d %H:%M:%S')
            sensor_id = f"SENS-{random.randint(1000, 9999)}"
            sensor_type = random.choice(sensor_types)
            location = random.choice(locations)
            
            # Generate appropriate values based on sensor type
            if sensor_type == 'temperature':
                value = round(random.uniform(18.0, 28.0), 1)
            elif sensor_type == 'humidity':
                value = round(random.uniform(30.0, 70.0), 1)
            elif sensor_type == 'pressure':
                value = round(random.uniform(980.0, 1020.0), 1)
            elif sensor_type == 'light':
                value = round(random.uniform(200, 800), 0)
            else:  # co2
                value = round(random.uniform(400, 1200), 0)
                
            battery_level = random.randint(10, 100)
            status = 'active' if random.random() > 0.05 else 'error'
            
            writer.writerow([timestamp, sensor_id, sensor_type, location, value, battery_level, status])
    
    print(f"Generated {num_entries} sensor data entries in {sensor_file}")

if __name__ == "__main__":
    print("Starting data collection and generation...")
    
    # Generate web logs
    generate_web_logs(2000)
    
    # Generate social media data
    generate_social_data(1000)
    
    # Generate sensor data
    generate_sensor_data(3000)
    
    print("Data collection and generation completed.")