-- Create database for our project
CREATE DATABASE IF NOT EXISTS bigdata_project;
USE bigdata_project;

-- Create external table for web logs
CREATE EXTERNAL TABLE IF NOT EXISTS web_logs (
    timestamp TIMESTAMP,
    ip_address STRING,
    user_id STRING,
    method STRING,
    endpoint STRING,
    status_code INT,
    response_time FLOAT,
    user_agent STRING,
    date DATE,
    hour INT,
    is_error BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/raw_data/web_logs_processed.csv'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create external table for social data
CREATE EXTERNAL TABLE IF NOT EXISTS social_data (
    post_id STRING,
    user_handle STRING,
    timestamp TIMESTAMP,
    content STRING,
    likes INT,
    shares INT,
    comments INT,
    sentiment STRING,
    category STRING,
    platform STRING,
    date DATE,
    hour INT,
    engagement_score FLOAT,
    sentiment_score INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/raw_data/social_data_processed.csv'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create external table for sensor data
CREATE EXTERNAL TABLE IF NOT EXISTS sensor_data (
    timestamp TIMESTAMP,
    sensor_id STRING,
    sensor_type STRING,
    location STRING,
    value FLOAT,
    battery_level INT,
    status STRING,
    date DATE,
    hour INT,
    is_active BOOLEAN,
    battery_category STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/raw_data/sensor_data_processed.csv'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create aggregated table for web traffic analytics
CREATE TABLE IF NOT EXISTS web_traffic_daily AS
SELECT 
    date,
    endpoint,
    COUNT(*) as total_requests,
    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) as error_count,
    AVG(response_time) as avg_response_time,
    COUNT(DISTINCT ip_address) as unique_visitors
FROM web_logs
GROUP BY date, endpoint;

-- Create aggregated table for hourly web traffic
CREATE TABLE IF NOT EXISTS web_traffic_hourly AS
SELECT 
    date,
    hour,
    COUNT(*) as total_requests,
    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) as error_count,
    AVG(response_time) as avg_response_time
FROM web_logs
GROUP BY date, hour;

-- Create aggregated table for social media engagement
CREATE TABLE IF NOT EXISTS social_engagement_daily AS
SELECT 
    date,
    platform,
    category,
    COUNT(*) as post_count,
    SUM(likes) as total_likes,
    SUM(shares) as total_shares,
    SUM(comments) as total_comments,
    AVG(engagement_score) as avg_engagement,
    AVG(sentiment_score) as avg_sentiment
FROM social_data
GROUP BY date, platform, category;

-- Create aggregated table for sensor readings
CREATE TABLE IF NOT EXISTS sensor_readings_daily AS
SELECT 
    date,
    sensor_type,
    location,
    COUNT(*) as reading_count,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_readings,
    SUM(CASE WHEN NOT is_active THEN 1 ELSE 0 END) as error_readings
FROM sensor_data
GROUP BY date, sensor_type, location;

-- Create joined table for correlation analysis between web traffic and social engagement
CREATE TABLE IF NOT EXISTS traffic_social_correlation AS
SELECT 
    w.date,
    w.total_requests,
    w.error_count,
    w.avg_response_time,
    s.post_count,
    s.total_likes,
    s.total_shares,
    s.total_comments,
    s.avg_engagement,
    s.avg_sentiment
FROM 
    (SELECT date, SUM(total_requests) as total_requests, SUM(error_count) as error_count, AVG(avg_response_time) as avg_response_time
     FROM web_traffic_daily GROUP BY date) w
JOIN 
    (SELECT date, COUNT(*) as post_count, SUM(total_likes) as total_likes, 
            SUM(total_shares) as total_shares, SUM(total_comments) as total_comments,
            AVG(avg_engagement) as avg_engagement, AVG(avg_sentiment) as avg_sentiment
     FROM social_engagement_daily GROUP BY date) s
ON w.date = s.date;

-- Export results for visualization
-- These queries will generate CSV files that can be used for visualization

-- Export web traffic by day and endpoint
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/exports/web_traffic_by_endpoint'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM web_traffic_daily;

-- Export web traffic by hour
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/exports/web_traffic_hourly'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM web_traffic_hourly;

-- Export social engagement by platform and category
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/exports/social_engagement'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM social_engagement_daily;

-- Export sensor readings
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/exports/sensor_readings'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM sensor_readings_daily;

-- Export correlation data
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/exports/correlation_data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM traffic_social_correlation;