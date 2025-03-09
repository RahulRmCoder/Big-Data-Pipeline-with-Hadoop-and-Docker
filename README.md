# Big Data Pipeline with Hadoop and Docker

This project implements a complete data processing pipeline using Hadoop and Docker. It collects and processes both structured and unstructured data from multiple synthetic sources, stores it in HDFS, processes it using Python, and creates visualizations.

## Project Overview

This project demonstrates a complete big data processing workflow using:
- **Hadoop** for distributed storage (HDFS)
- **Docker** for containerization and easy setup
- **Python** for data collection, processing, and visualization
- **Pandas, Matplotlib, Seaborn, and Plotly** for data analysis and visualization

The pipeline handles both structured data (sensor readings with fixed schemas) and unstructured/semi-structured data (social media content, log files) through a comprehensive ETL process. The direct processing approach demonstrates how Python can be used as an alternative to Hive for processing diverse data types in a big data environment.

## Prerequisites

- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/install/)
- [Python 3.6+](https://www.python.org/downloads/)
- Required Python packages: `pandas`, `matplotlib`, `seaborn`, `numpy`, `plotly`, `faker`

## Project Structure

```
hadoop-hive-project/
│
├── data/                         # Data directory
│   ├── raw/                      # Raw data (logs, social media, sensors)
│   ├── processed/                # Processed data ready for HDFS
│   ├── exports/                  # Data exported from processing
│   └── visualization/            # Data prepared for visualization
│
├── docker-compose.yml            # Docker configuration for Hadoop
├── hadoop.env                    # Environment variables for Hadoop
│
├── scripts/                      # Python scripts
│   ├── collect_data.py           # Script to collect/generate data
│   ├── process_data.py           # Script to process data and upload to HDFS
│   ├── direct_processing.py      # Script for direct data processing
│   └── prepare_for_visualization.py # Prepare data for visualization
│
├── notebooks/                    # Jupyter notebooks
│   └── visualizations.ipynb      # Interactive data visualization
│
├── visualization/                # Visualization outputs
│
├── run_pipeline.sh               # Shell script to run the entire pipeline
└── README.md                     # Project documentation
```

## Installation and Setup

### 1. Clone the Repository

```bash
git clone https://github.com/RahulRmCoder/Big-Data-Pipeline-with-Hadoop-and-Docker.git
cd Big-Data-Pipeline-with-Hadoop-and-Docker
```

### 2. Set Up Python Environment

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/Scripts/activate  # On Windows
# OR
source .venv/bin/activate      # On Linux/Mac

# Install required packages
pip install pandas matplotlib seaborn numpy plotly faker jupyter
```

### 3. Start Docker Containers

```bash
docker-compose up -d
```

This will start the Hadoop ecosystem including:
- Hadoop NameNode and DataNode
- YARN ResourceManager and NodeManager
- Supporting services

## Running the Pipeline

### Option 1: Run the Entire Pipeline

The entire pipeline can be executed using the provided shell script:

```bash
# Make the script executable
chmod +x run_pipeline.sh

# Run the pipeline
./run_pipeline.sh
```

This script will:
1. Start the Docker containers
2. Generate synthetic data
3. Process the data and upload it to HDFS
4. Perform data processing and analysis
5. Prepare the data for visualization

### Option 2: Run Each Step Individually

If you prefer to run each step separately:

#### 1. Data Collection

```bash
python scripts/collect_data.py
```

This generates synthetic data for:
- Web server logs
- Social media data
- IoT sensor readings

#### 2. Data Processing

```bash
python scripts/process_data.py
```

This processes the raw data and uploads it to HDFS.

#### 3. Direct Data Processing

```bash
python scripts/direct_processing.py
```

This performs data aggregation and analysis.

#### 4. Visualization Preparation

```bash
python scripts/prepare_for_visualization.py
```

This prepares the processed data for visualization.

#### 5. Run Visualizations

```bash
jupyter notebook notebooks/visualizations.ipynb
```

This opens the Jupyter notebook for interactive visualizations.

## Data Sources and Processing

The project uses and processes both structured and unstructured data:

### 1. Web Server Logs (Semi-structured)
- Timestamps, IP addresses, endpoints, status codes, etc.
- Tracks user interactions and system performance
- Processed from raw log format into structured analytical data

### 2. Social Media Data (Unstructured/Semi-structured)
- Posts, engagement metrics (likes, shares, comments)
- Text content with sentiment analysis and categorization
- Processes free-form text content alongside structured engagement metrics

### 3. IoT Sensor Data (Structured)
- Temperature, humidity, pressure readings
- Sensor status and battery levels
- Highly structured time-series data with well-defined schemas

## Visualization Outputs

The pipeline generates various visualizations:

1. **Web Traffic Analysis**
   - Traffic distribution by endpoint
   - Error rates analysis
   - Hourly traffic patterns

2. **Social Media Analysis**
   - Engagement metrics by platform
   - Sentiment analysis
   - Temporal engagement patterns

3. **Sensor Data Analysis**
   - Reading distributions by sensor type
   - Error rate analysis
   - Time series of sensor values

4. **Correlation Analysis**
   - Relationships between web traffic and social engagement
   - Cross-domain pattern identification

## Access to Services

After starting the containers, you can access:

- Hadoop UI: http://localhost:9870
- YARN UI: http://localhost:8088
- HDFS directories via commands:
  ```bash
  docker exec namenode hdfs dfs -ls /user/hive/warehouse
  ```

## Troubleshooting

### Docker Issues
- If containers fail to start, check logs: `docker-compose logs`
- Ensure no port conflicts with services running on your machine

### HDFS Issues
- If data upload fails, check HDFS permissions: 
  ```bash
  docker exec namenode hdfs dfs -chmod -R 777 /user/hive/warehouse
  ```

### Visualization Issues
- If visualizations don't load in Jupyter, ensure all data files exist in the expected locations
- Check that all Python packages are installed correctly

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Docker and Hadoop communities for providing robust tools
- The open-source data science ecosystem in Python
