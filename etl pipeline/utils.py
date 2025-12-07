# utils.py – Helper Functions for NYC MTA Dashboard

import pandas as pd
import numpy as np
from scipy import stats
import plotly.graph_objects as go


def interpret_correlation(r: float) -> str:
    """
    Interpret correlation coefficient strength and direction.
    
    Args:
        r (float): Correlation coefficient.
        
    Returns:
        str: Description of correlation strength and direction.
    """
    abs_r = abs(r)
    if abs_r > 0.7:
        strength = "Very Strong"
    elif abs_r > 0.5:
        strength = "Strong"
    elif abs_r > 0.3:
        strength = "Moderate"
    elif abs_r > 0.1:
        strength = "Weak"
    else:
        strength = "Very Weak"
    
    direction = "Positive" if r > 0 else "Negative"
    return f"{strength} {direction}"


def create_qq_plot(data: pd.Series, title: str) -> go.Figure | None:
    """
    Create a Q-Q plot for normality assessment using Plotly.
    
    Args:
        data (pd.Series): Numeric data series.
        title (str): Title for the plot.
        
    Returns:
        go.Figure | None: Plotly figure or None if insufficient data.
    """
    try:
        data_clean = data.dropna()
        if len(data_clean) < 10:
            return None

        qq = stats.probplot(data_clean, dist="norm")
        
        fig = go.Figure()

        # Add scatter points
        fig.add_trace(go.Scatter(
            x=qq[0][0],
            y=qq[0][1],
            mode='markers',
            name='Data',
            marker=dict(color='blue', size=4)
        ))

        # Add reference line
        fig.add_trace(go.Scatter(
            x=qq[0][0],
            y=qq[1][0] * qq[0][0] + qq[1][1],
            mode='lines',
            name='Normal',
            line=dict(color='red', dash='dash')
        ))

        fig.update_layout(
            title=f"Q-Q Plot: {title}",
            xaxis_title="Theoretical Quantiles",
            yaxis_title="Sample Quantiles",
            showlegend=True,
            height=300
        )

        return fig
    except Exception as e:
        print(f"Error creating Q-Q plot: {e}")
        return None


def safe_divide(numerator: float, denominator: float) -> float:
    """
    Safely divide two numbers, returning np.nan if denominator is zero.
    
    Args:
        numerator (float): Numerator.
        denominator (float): Denominator.
        
    Returns:
        float: Result of division or np.nan.
    """
    try:
        return numerator / denominator if denominator != 0 else np.nan
    except Exception:
        return np.nan


def convert_fahrenheit(celsius: float) -> float:
    """
    Convert Celsius to Fahrenheit.
    
    Args:
        celsius (float): Temperature in Celsius.
        
    Returns:
        float: Temperature in Fahrenheit.
    """
    try:
        return celsius * 9/5 + 32
    except Exception:
        return np.nan


def convert_inches(mm: float) -> float:
    """
    Convert millimeters to inches.
    
    Args:
        mm (float): Precipitation in millimeters.
        
    Returns:
        float: Precipitation in inches.
    """
    try:
        return mm / 25.4
    except Exception:
        return np.nan
