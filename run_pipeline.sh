#!/bin/bash

# Big Data Pipeline Script
# This script runs the entire pipeline from data collection to visualization

# Define color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print section header
print_header() {
    echo -e "\n${YELLOW}===============================================${NC}"
    echo -e "${YELLOW}$1${NC}"
    echo -e "${YELLOW}===============================================${NC}\n"
}

# Function to check if previous command was successful
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1 completed successfully${NC}"
    else
        echo -e "${RED}✗ $1 failed${NC}"
        if [ "$2" == "exit" ]; then
            echo -e "${RED}Exiting pipeline...${NC}"
            exit 1
        fi
    fi
}

# Set the current directory to the project root
cd "$(dirname "$0")"

# Step 1: Start Docker containers
print_header "STEP 1: Starting Docker Containers"
docker-compose up -d
check_status "Starting Docker containers" "exit"

# Wait for services to be ready
echo "Waiting for services to start (30 seconds)..."
sleep 30

# Step 2: Collect data
print_header "STEP 2: Collecting Data"
# Using py command which works better on Windows
py scripts/collect_data.py
check_status "Data collection" "exit"

# Step 3: Process data and upload to HDFS
print_header "STEP 3: Processing Data and Uploading to HDFS"
py scripts/process_data.py
check_status "Data processing and HDFS upload" "exit"

# Step 4: Create directories in HDFS (fixed command for Windows)
print_header "STEP 4: Setting up HDFS Directories"
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod g+w /user/hive/warehouse
check_status "HDFS directory setup"

# Step 5: Direct Data Processing (Alternative to Hive)
print_header "STEP 5: Direct Data Processing"
py scripts/direct_processing.py
check_status "Direct data processing" "exit"

# Step 6: Prepare Data for Visualization
print_header "STEP 6: Preparing Data for Visualization"
py scripts/prepare_for_visualization.py
check_status "Preparing data for visualization"

# Step 7: Generate visualizations
print_header "STEP 7: Generating Visualizations"
# This would typically call a visualization script or tool
echo "Please open the Jupyter notebook 'notebooks/visualizations.ipynb' to create visualizations"
echo "Or use your preferred visualization tool with the data in 'data/visualization/'"

print_header "PIPELINE COMPLETED SUCCESSFULLY"
echo "The following output files were created:"
ls -l data/exports/
echo -e "\nVisualization data prepared in data/visualization/ directory"
echo -e "\nTo access Hadoop UI: http://localhost:9870"
echo "To access YARN UI: http://localhost:8088"
echo "To access Hive: http://localhost:10002"