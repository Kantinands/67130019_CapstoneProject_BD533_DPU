# Capstone Project: Bangkok AQI Data Pipeline

This Airflow DAG is part of a Capstone project to build  data pipeline for collecting, transforming, storing, and analyzing **Air Quality Index (AQI)** data for **Bangkok, Thailand**.

## 🛠 Project Objective
To automate the process of:
- Extracting AQI data from the **AirVisual API**
- Transforming and validating the data
- Storing it in a **PostgreSQL** database
- Summarizing and generating **business insights** for decision-making

## 📅 Final DAG Name
**`mycapstoneproject_ver2`**  
(for **`mycapstoneproject_dbt`** is a not complete and not success in dbt method)

### ⏰ Schedule
Runs every 6 hours: `0 */6 * * *`
---
## 📌 Tasks Breakdown

### 1. `extract_aqi_data`
- Calls the [AirVisual API](https://www.iqair.com/world-air-quality-api) for AQI data in Bangkok
- Pushes raw JSON data to XCom for downstream use
### 2. `transform_aqi_data`
- Extracts relevant fields: `timestamp`, `aqi`, and `retrieval time`
- Converts into a simplified format and pushes to XCom
### 3. `check_data_quality`
- Ensures all required fields are present
- Validates AQI values (must be 0–1000)
- Raises errors if data is invalid
### 4. `load_to_postgres`
- Creates and inserts into table: `aqi_data`
- Stores each AQI data point with its timestamp and retrieval time
### 5. `summarize_aqi_data`
- Creates or updates the table: `aqi_summary`
- Aggregates daily AQI stats: **max, min, avg, count**
### 6. `generate_business_insights`
- Creates schema and table: `business_insight.aqi_insights`
- Calculates insights:
  - Highest AQI this week
  - Lowest AQI in the last 3 months
  - Average AQI this week
  - Worst AQI day in the last month
  - Count of unhealthy AQI days in the last 30 days


## 🚀 How to Use

1. Add this DAG script to Airflow DAGs folder
2. Set API key as an environment variable (.env)
3. Start Airflow scheduler and webserver(postgres)
4. Trigger the DAG run on schedule 
---

## 🎥 Youtube Explanation 
https://www.youtube.com/watch?v=WCs6AyghLs8

## BD533 Data pipeline Project
Kantinan Rodphaibool 67130019
