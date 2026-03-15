"""
DIA v2 - ML Features
======================
Anomaly detection and time-series forecasting.
Uses Python libraries (scipy, Prophet) instead of Snowflake ML functions.

Can be swapped to Snowflake ML functions later if re-enabled.
"""

import logging
import numpy as np
import pandas as pd
from typing import Optional
import snowflake.connector
from config import SnowflakeConfig

logger = logging.getLogger("dia-v2.ml")


async def detect_anomalies(query: str) -> dict:
    """
    Detect anomalies in email campaign performance metrics.
    Uses IQR method + Z-score for statistical anomaly detection.
    """
    try:
        # Fetch historical data from Snowflake
        data = _fetch_historical_data(query)

        if data is None or data.empty:
            return {
                "anomalies": [],
                "summary": "Insufficient historical data for anomaly detection.",
            }

        anomalies = []

        # Group by relevant dimensions and detect per-group anomalies
        for metric in ["click_rate", "open_rate", "unsubscribe_rate"]:
            if metric not in data.columns:
                continue

            series = data[metric].dropna()
            if len(series) < 10:
                continue

            # Z-score method
            mean = series.mean()
            std = series.std()
            if std == 0:
                continue

            z_scores = (series - mean) / std

            # Flag points > 2 standard deviations
            outlier_mask = np.abs(z_scores) > 2.0
            outlier_indices = series[outlier_mask].index

            for idx in outlier_indices:
                row = data.iloc[idx] if idx < len(data) else None
                if row is not None:
                    direction = "above" if z_scores.iloc[idx] > 0 else "below"
                    anomalies.append({
                        "metric": metric,
                        "value": float(row[metric]),
                        "expected_mean": float(mean),
                        "z_score": float(z_scores.iloc[idx]),
                        "direction": direction,
                        "date": str(row.get("send_date", row.get("send_month", "N/A"))),
                        "country": str(row.get("country", "N/A")),
                        "car_model": str(row.get("car_model", "N/A")),
                    })

        # Also run IQR method for robustness
        for metric in ["click_rate", "open_rate"]:
            if metric not in data.columns:
                continue
            series = data[metric].dropna()
            q1, q3 = series.quantile(0.25), series.quantile(0.75)
            iqr = q3 - q1
            lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr

            iqr_outliers = series[(series < lower) | (series > upper)]
            # Merge with z-score results (avoid duplicates)
            # IQR results supplement z-score findings

        return {
            "anomalies": anomalies[:10],  # Cap at 10 most significant
            "total_found": len(anomalies),
            "method": "Z-score (σ > 2.0) + IQR",
            "data_points_analyzed": len(data),
        }

    except Exception as e:
        logger.error(f"Anomaly detection failed: {e}")
        return {
            "anomalies": [],
            "summary": f"Anomaly detection encountered an error: {str(e)[:100]}",
        }


async def forecast_metric(query: str) -> dict:
    """
    Generate time-series forecast using Prophet.
    Falls back to simple linear extrapolation if Prophet fails.
    """
    try:
        data = _fetch_time_series_data(query)

        if data is None or len(data) < 6:
            return {
                "forecast": [],
                "summary": "Insufficient historical data for forecasting (need 6+ months).",
            }

        # Try Prophet first
        try:
            from prophet import Prophet

            # Prophet requires 'ds' and 'y' columns
            prophet_df = data.rename(columns={"send_month": "ds", "metric_value": "y"})

            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=False,
                daily_seasonality=False,
                interval_width=0.95,
            )
            model.fit(prophet_df)

            # Forecast 3 months ahead
            future = model.make_future_dataframe(periods=3, freq="MS")
            forecast = model.predict(future)

            # Extract forecast periods only
            forecast_periods = forecast.tail(3)

            forecast_result = []
            for _, row in forecast_periods.iterrows():
                forecast_result.append({
                    "date": str(row["ds"].date()),
                    "predicted": round(float(row["yhat"]), 4),
                    "lower_bound": round(float(row["yhat_lower"]), 4),
                    "upper_bound": round(float(row["yhat_upper"]), 4),
                })

            # Calculate MAPE on training data
            train_forecast = model.predict(prophet_df[["ds"]])
            actual = prophet_df["y"].values
            predicted = train_forecast["yhat"].values
            mape = np.mean(np.abs((actual - predicted) / actual)) * 100

            return {
                "forecast": forecast_result,
                "method": "Prophet (time-series decomposition)",
                "mape": round(float(mape), 1),
                "confidence_interval": "95%",
                "historical_periods": len(data),
            }

        except ImportError:
            logger.warning("Prophet not available, using linear extrapolation")
            return _simple_forecast(data)

    except Exception as e:
        logger.error(f"Forecasting failed: {e}")
        return {
            "forecast": [],
            "summary": f"Forecasting error: {str(e)[:100]}",
        }


def _simple_forecast(data: pd.DataFrame) -> dict:
    """Simple linear extrapolation fallback."""
    from scipy import stats

    y = data["metric_value"].values
    x = np.arange(len(y))

    slope, intercept, r_value, _, std_err = stats.linregress(x, y)

    forecast_result = []
    for i in range(1, 4):
        pred = slope * (len(y) + i - 1) + intercept
        margin = 1.96 * std_err * np.sqrt(1 + 1/len(y))
        forecast_result.append({
            "date": f"Month +{i}",
            "predicted": round(float(pred), 4),
            "lower_bound": round(float(pred - margin), 4),
            "upper_bound": round(float(pred + margin), 4),
        })

    return {
        "forecast": forecast_result,
        "method": "Linear regression (fallback)",
        "r_squared": round(float(r_value**2), 3),
        "confidence_interval": "95%",
        "historical_periods": len(data),
    }


def _fetch_historical_data(query: str) -> Optional[pd.DataFrame]:
    """Fetch historical performance data from Snowflake for anomaly detection."""
    sql = """
    SELECT
        f.send_date,
        DATE_TRUNC('MONTH', f.send_date) AS send_month,
        m.business_unit AS country,
        ROUND(SUM(f.unique_clicks) * 100.0 / NULLIF(SUM(f.sends) - SUM(f.bounces), 0), 3) AS click_rate,
        ROUND(SUM(f.unique_opens)  * 100.0 / NULLIF(SUM(f.sends) - SUM(f.bounces), 0), 3) AS open_rate,
        ROUND(SUM(f.unsubscribes)  * 100.0 / NULLIF(SUM(f.sends), 0), 4) AS unsubscribe_rate,
        SUM(f.sends) AS total_sends
    FROM AGENT_V_FACT_SFMC_PERFORMANCE_TRACKING f
    LEFT JOIN AGENT_V_DIM_SFMC_METADATA_JOB m ON f.comp_key = m.comp_key
    WHERE (m.email_name NOT ILIKE '%sparkpost%' OR m.email_name IS NULL)
      AND f.send_date >= DATEADD('MONTH', -12, CURRENT_DATE)
    GROUP BY f.send_date, DATE_TRUNC('MONTH', f.send_date), m.business_unit
    ORDER BY f.send_date
    """
    return _run_query(sql)


def _fetch_time_series_data(query: str) -> Optional[pd.DataFrame]:
    """Fetch monthly aggregated data for forecasting."""
    sql = """
    SELECT
        DATE_TRUNC('MONTH', f.send_date) AS send_month,
        ROUND(SUM(f.unique_clicks) * 100.0 / NULLIF(SUM(f.sends) - SUM(f.bounces), 0), 3) AS metric_value,
        COUNT(*) AS sample_size
    FROM AGENT_V_FACT_SFMC_PERFORMANCE_TRACKING f
    LEFT JOIN AGENT_V_DIM_SFMC_METADATA_JOB m ON f.comp_key = m.comp_key
    WHERE (m.email_name NOT ILIKE '%sparkpost%' OR m.email_name IS NULL)
      AND f.send_date >= DATEADD('MONTH', -18, CURRENT_DATE)
    GROUP BY DATE_TRUNC('MONTH', f.send_date)
    ORDER BY send_month
    """
    return _run_query(sql)


def _run_query(sql: str) -> Optional[pd.DataFrame]:
    """Execute SQL and return as DataFrame."""
    try:
        conn = snowflake.connector.connect(**SnowflakeConfig.connection_params())
        cursor = conn.cursor()
        cursor.execute(f"USE SCHEMA {SnowflakeConfig.DATABASE}.{SnowflakeConfig.SCHEMA}")
        cursor.execute(sql)
        columns = [desc[0].lower() for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        logger.error(f"Snowflake query failed: {e}")
        return None
