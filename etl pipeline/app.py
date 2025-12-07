
# Load helpful libraries

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
from datetime import datetime
import os
from data_extraction import DataExtractor
from data_transformation import DataTransformer

st.set_page_config(page_title="NYC MTA Ridership + Weather Dashboard", layout="wide")
st.title("NYC MTA Ridership + Weather Analysis Dashboard")

CACHE_FILE = "merged_data.parquet"

# LOAD + CACHE DATA

@st.cache_data(show_spinner=True)
def load_data():
    extractor = DataExtractor()
    transformer = DataTransformer()

    if os.path.exists(CACHE_FILE):
        st.info("Loading cached merged data...")
        merged_df = pd.read_parquet(CACHE_FILE)
        quality_report = transformer.get_quality_report()
        
        # CRITICAL FIX: Check if conversions exist, if not, reprocess
        needs_reprocess = False
        if 'temperature_f' not in merged_df.columns:
            st.warning("Cached data missing Fahrenheit conversion - reprocessing...")
            needs_reprocess = True
        if 'precipitation_in' not in merged_df.columns:
            st.warning("Cached data missing inches conversion - reprocessing...")
            needs_reprocess = True
            
        if needs_reprocess:
            os.remove(CACHE_FILE)
            st.info("Regenerating data with proper unit conversions...")
            # Fall through to fetch fresh data below

    if not os.path.exists(CACHE_FILE):
        st.info("Fetching fresh data with unit conversions...")

        ridership_df = extractor.fetch_ridership_data(
            start_date="2023-01-01",
            end_date="2024-12-31",
            max_records=600000
        )
        weather_df = extractor.fetch_weather_data(
            start_date="2023-01-01",
            end_date="2024-12-31"
        )

        # Transform and merge (this applies unit conversions)
        merged_df = transformer.transform_and_merge(ridership_df, weather_df)

        # Save to cache
        merged_df.to_parquet(CACHE_FILE, index=False)
        quality_report = transformer.get_quality_report()
        
        # Verify conversions were applied
        if 'temperature_f' in merged_df.columns:
            st.success(f"Temperature converted: {merged_df['temperature_f'].mean():.1f}°F average")
        if 'precipitation_in' in merged_df.columns:
            st.success(f"Precipitation converted: {merged_df['precipitation_in'].mean():.3f}in average")

    # Precompute for filters
    merged_df["ridership"] = pd.to_numeric(merged_df["ridership"], errors="coerce")
    merged_df["year"] = merged_df["date"].dt.year
    merged_df["month"] = merged_df["date"].dt.month
    merged_df["month_name"] = merged_df["date"].dt.strftime("%B")
    merged_df["day_name"] = merged_df["date"].dt.day_name()

    return merged_df, quality_report


with st.spinner("Loading data..."):
    merged_df, quality_report = load_data()

if merged_df.empty:
    st.error("No data returned from extraction.")
    st.stop()

st.success(f"Loaded {len(merged_df):,} total records")

# Display unit confirmation
col_info1, col_info2 = st.columns(2)
with col_info1:
    if 'temperature_f' in merged_df.columns:
        st.metric("Temperature Unit", "°F (Fahrenheit)")
    else:
        st.warning("Temperature not converted to Fahrenheit")

with col_info2:
    if 'precipitation_in' in merged_df.columns:
        st.metric("Precipitation Unit", "inches")
    else:
        st.warning("Precipitation not converted to inches")


# SIDEBAR FILTERS

st.sidebar.header("Filters")

years_available = sorted(merged_df["year"].unique())
months_available = merged_df["month_name"].unique()
days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

selected_years = st.sidebar.multiselect("Year", years_available, years_available)
selected_months = st.sidebar.multiselect("Month", months_available, months_available)
selected_days = st.sidebar.multiselect("Day of Week", days, days)

date_range = st.sidebar.date_input(
    "Date Range",
    value=[merged_df["date"].min(), merged_df["date"].max()],
    min_value=merged_df["date"].min().date(),
    max_value=merged_df["date"].max().date()
)


# APPLY FILTERS

filtered_df = merged_df[
    (merged_df["year"].isin(selected_years)) &
    (merged_df["month_name"].isin(selected_months)) &
    (merged_df["day_name"].isin(selected_days)) &
    (merged_df["date"] >= pd.to_datetime(date_range[0])) &
    (merged_df["date"] <= pd.to_datetime(date_range[1]))
].copy()


# SUMMARY STATISTICS

st.header("Overview of Selected Period")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Filtered Records", f"{len(filtered_df):,}")

max_r = filtered_df["ridership"].max()
col3.metric("Max Ridership", f"{max_r:,.0f}" if not pd.isna(max_r) else "–")

if len(filtered_df) > 0:
    span = (filtered_df["date"].max() - filtered_df["date"].min()).days
else:
    span = "–"
col4.metric("Date Span (days)", span)

# Additional weather metrics
st.subheader("Weather Summary")
col1, col2, col3 = st.columns(3)

if 'temperature_f' in filtered_df.columns:
    avg_temp = filtered_df['temperature_f'].mean()
    col1.metric("Avg Temperature", f"{avg_temp:.1f}°F" if not pd.isna(avg_temp) else "–")
else:
    col1.metric("Avg Temperature", "Not Available")

if 'precipitation_in' in filtered_df.columns:
    avg_precip = filtered_df['precipitation_in'].mean()
    col2.metric("Avg Precipitation", f"{avg_precip:.3f}in" if not pd.isna(avg_precip) else "–")
else:
    col2.metric("Avg Precipitation", "Not Available")

if 'windspeed_mph' in filtered_df.columns:
    avg_wind = filtered_df['windspeed_mph'].mean()
    col3.metric("Avg Wind Speed", f"{avg_wind:.1f} mph" if not pd.isna(avg_wind) else "–")
elif 'windspeed' in filtered_df.columns:
    avg_wind = filtered_df['windspeed'].mean()
    col3.metric("Avg Wind Speed", f"{avg_wind:.1f} m/s" if not pd.isna(avg_wind) else "–")


# SECTION: RIDERSHIP + WEATHER TOGETHER (DUAL-AXIS)

st.header("Ridership and Weather Together Over Time")

fig_combined = px.line()

# Ridership (primary axis)
fig_combined.add_scatter(
    x=filtered_df["date"],
    y=filtered_df["ridership"],
    mode="lines",
    name="Ridership",
    line=dict(color='blue')
)

# Temperature (secondary axis) - USE FAHRENHEIT
temp_col = 'temperature_f' if 'temperature_f' in filtered_df.columns else 'temperature_mean'
temp_label = 'Temperature (°F)' if 'temperature_f' in filtered_df.columns else 'Temperature (°C)'

fig_combined.add_scatter(
    x=filtered_df["date"],
    y=filtered_df[temp_col],
    mode="lines",
    name=temp_label,
    yaxis="y2",
    line=dict(color='red')
)

fig_combined.update_layout(
    title="Ridership vs Temperature Over Time",
    yaxis=dict(title="Ridership"),
    yaxis2=dict(
        title=temp_label,
        overlaying="y",
        side="right"
    ),
    legend=dict(orientation="h", y=-0.15)
)

st.plotly_chart(fig_combined, use_container_width=True)

# SECTION: RIDERSHIP TRENDS

st.header("How Ridership Changes Over Time")

fig = px.line(
    filtered_df,
    x="date",
    y="ridership",
    color="year",
    title="Daily Ridership Trend",
    labels={"ridership": "Ridership", "date": "Date"}
)
st.plotly_chart(fig, use_container_width=True)


# SECTION: WEATHER CONDITIONS (USING CONVERTED UNITS)

st.header("🌦 Weather Patterns Over Time")

# Build weather columns list dynamically
weather_cols = []
weather_labels = {}

if 'temperature_f' in filtered_df.columns:
    weather_cols.append('temperature_f')
    weather_labels['temperature_f'] = 'Temperature (°F)'
elif 'temperature_mean' in filtered_df.columns:
    weather_cols.append('temperature_mean')
    weather_labels['temperature_mean'] = 'Temperature (°C)'

if 'precipitation_in' in filtered_df.columns:
    weather_cols.append('precipitation_in')
    weather_labels['precipitation_in'] = 'Precipitation (in)'
elif 'precipitation' in filtered_df.columns:
    weather_cols.append('precipitation')
    weather_labels['precipitation'] = 'Precipitation (mm)'

if weather_cols:
    fig_weather = px.line(
        filtered_df,
        x="date",
        y=weather_cols,
        title="Temperature & Precipitation Over Time",
        labels=weather_labels
    )
    st.plotly_chart(fig_weather, use_container_width=True)
else:
    st.warning("Weather data not available")


# SECTION: RIDERSHIP BEHAVIOR BY DAY OF WEEK

st.header("Weekly Behavior: Average Ridership by Day")

avg_by_day = (
    filtered_df.groupby("day_name")["ridership"]
    .mean()
    .reindex(days)
    .reset_index()
)

fig_wd = px.bar(
    avg_by_day,
    x="day_name",
    y="ridership",
    color="ridership",
    title="Average Ridership by Day of Week",
    color_continuous_scale="Blues"
)
st.plotly_chart(fig_wd, use_container_width=True)


# SECTION: CORRELATION ANALYSIS

st.header("Does Weather Affect Ridership? (Correlation Analysis)")

# Select relevant numeric columns (prefer converted units)
numeric_cols = ["ridership"]

if 'temperature_f' in filtered_df.columns:
    numeric_cols.append("temperature_f")
elif 'temperature_mean' in filtered_df.columns:
    numeric_cols.append("temperature_mean")

if 'precipitation_in' in filtered_df.columns:
    numeric_cols.append("precipitation_in")
elif 'precipitation' in filtered_df.columns:
    numeric_cols.append("precipitation")

# Create correlation matrix
corr_df = filtered_df[numeric_cols].corr()

fig_corr = ff.create_annotated_heatmap(
    z=corr_df.values,
    x=corr_df.columns.tolist(),
    y=corr_df.columns.tolist(),
    showscale=True,
    colorscale='RdBu'
)

fig_corr.update_layout(title="Correlation Heatmap (Converted Units)")
st.plotly_chart(fig_corr, use_container_width=True)

# Scatter matrix
st.subheader("Ridership + Weather Scatter Matrix")

# Determine color column
color_col = 'temperature_f' if 'temperature_f' in filtered_df.columns else 'temperature_mean'

fig_scatter = px.scatter_matrix(
    filtered_df,
    dimensions=numeric_cols,
    color=color_col,
    title="Relationships Between Ridership and Weather Variables (Converted Units)"
)
st.plotly_chart(fig_scatter, use_container_width=True)

# WEATHER CATEGORIES ANALYSIS

if 'temp_category' in filtered_df.columns:
    st.header("Ridership by Temperature Category")
    
    temp_ridership = filtered_df.groupby('temp_category')['ridership'].mean().reset_index()
    
    fig_temp = px.bar(
        temp_ridership,
        x='temp_category',
        y='ridership',
        title="Average Ridership by Temperature Category",
        labels={'temp_category': 'Temperature Category', 'ridership': 'Average Ridership'},
        color='ridership',
        color_continuous_scale='RdYlBu_r'
    )
    st.plotly_chart(fig_temp, use_container_width=True)

if 'rain_category' in filtered_df.columns:
    st.header("Ridership by Precipitation Category")
    
    rain_ridership = filtered_df.groupby('rain_category')['ridership'].mean().reset_index()
    
    fig_rain = px.bar(
        rain_ridership,
        x='rain_category',
        y='ridership',
        title="Average Ridership by Precipitation Category",
        labels={'rain_category': 'Rain Category', 'ridership': 'Average Ridership'},
        color='ridership',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig_rain, use_container_width=True)


# SAMPLE DATA + QUALITY REPORT

st.header("Sample of Filtered Data")

# Select important columns to display
display_cols = ['date', 'ridership']
if 'temperature_f' in filtered_df.columns:
    display_cols.append('temperature_f')
if 'precipitation_in' in filtered_df.columns:
    display_cols.append('precipitation_in')
if 'day_name' in filtered_df.columns:
    display_cols.append('day_name')
if 'weather_condition' in filtered_df.columns:
    display_cols.append('weather_condition')

available_cols = [col for col in display_cols if col in filtered_df.columns]
st.dataframe(filtered_df[available_cols].head(100), use_container_width=True)

st.header("Data Quality Report")
with st.expander("View Report"):
    for name, metrics in quality_report.items():
        st.markdown(f"### {name.upper()}")
        st.table(pd.DataFrame(metrics.items(), columns=["Metric", "Value"]))


# DOWNLOAD DATA

st.header("Download Filtered Data")
st.download_button(
    "Download CSV",
    filtered_df.to_csv(index=False),
    file_name="filtered_ridership_data.csv",
    mime="text/csv"
)

# FOOTER

st.markdown("---")
st.caption(f"Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.caption("Data sources: NYC MTA Open Data, Open-Meteo Weather API")