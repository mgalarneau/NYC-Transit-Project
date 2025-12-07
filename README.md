# NYC MTA Ridership + Weather Dashboard

This **Streamlit-based dashboard** allows users to explore NYC MTA ridership alongside weather metrics through interactive visualizations, a data quality report, and filters for date, year, month, and day of the week.

Designed for teams in operations, finance, and planning, the app shows how weather influences transit performance. By combining historical ridership with weather data, it aims to enhance forecasting accuracy, support smarter service planning, prevent overcrowding, and reduce unnecessary trips, driving operational savings in staffing and fuel.

The ETL dashboard streamlines manual processes, establishes a scalable analytics foundation, and delivers insights on daily trends, weather impacts, and seasonal patterns, enabling data-driven decisions.

---

## Data Source and Analysis Process

### Data Sources

The data used for this project comes from:

1. [**MTA Ridership Data**](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/about_data): Hourly and daily ridership counts across NYC transit routes.
2. [**Weather Data**](https://open-meteo.com/en/docs/historical-weather-api): Historical weather data including temperature, precipitation, and other relevant metrics.

These datasets are fetched via APIs and merged to allow integrated analysis of ridership and weather trends over time.

### Analysis Process

The application performs the following steps:

1. **Data Extraction**: The `DataExtractor` module fetches raw ridership and weather data.
2. **Data Transformation**: The `DataTransformer` module merges datasets, converts data types, computes derived columns (e.g., year, month, day of week), and generates a **data quality report** including:

   * Total rows
   * Duplicate rows
   * Missing values per column
   * Column names
3. **Filtering**: Users can filter the data interactively by:

   * Year
   * Month
   * Day of the week
   * Date range
4. **Visualizations**: Interactive plots include:

   * Ridership over time
   * Ridership vs temperature
   * Average ridership by day of week
   * Correlation heatmap for numeric variables
   * Q-Q plots for normality inspection
5. **Data Export**: Users can download the filtered dataset as a CSV file.

---

## Prerequisites

Before you can set up and run this app, ensure you have the following software installed:

* Python 3.9+
* pip (Python package installer)
* Virtualenv (Optional but recommended)

---

## Setting Up on macOS and Windows

### 1. Clone the Repository

```bash
git clone https://github.com/mgalarneau/NYC-Transit-Project.git
cd NYC-Transit-Project
```

### 2. Create a Virtual Environment (Optional but Recommended)

**macOS:**

```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows:**

```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install the Dependencies

```bash
pip install -r requirements.txt
```

### 4. Running the Application

Run the Streamlit app with:

```bash
streamlit run app.py
```

By default, the app should open in your browser at `http://localhost:8501`.

---

## Features

### Interactive Filters

* Year, month, day of week
* Date range picker

### Data Quality Report

* Total rows
* Duplicate rows
* Missing values per column
* Column names

### Visualizations

* Ridership trends over time
* Ridership vs weather metrics
* Average ridership by day
* Correlation heatmaps
* Q-Q plots for normality checks

### Download Data

Filtered datasets can be downloaded as CSV files.

---

## Troubleshooting

### Common Issues

1. **Streamlit not opening**: Make sure you activated your virtual environment and installed all dependencies.
2. **Missing data columns**: Some weather or ridership metrics may not exist in the dataset; the app will handle missing columns gracefully.
3. **Performance with large datasets**: The app caches merged data to improve load times. Delete `merged_data.parquet` to force fresh extraction if needed.

---

## Testing

Tests can be run using `pytest`. Install dependencies first:

```bash
pip install -r requirements.txt
```

Then run:

```bash
pytest
```

---

# Deployment Instructions

The NYC MTA Ridership + Weather Dashboard can be deployed locally, on **Streamlit Community Cloud**

---

## 1. Running Locally

To run the app on your local machine:

1. Ensure Python 3.9+ is installed.
2. Install dependencies:

```bash
pip install -r requirements.txt

## 2. Run the streamlit app:

streamlit run app.py

