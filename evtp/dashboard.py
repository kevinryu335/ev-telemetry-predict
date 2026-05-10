# evtp/dashboard.py
from pathlib import Path
import sqlite3

import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh


# ------------------------------------------------------
# Page setup
# ------------------------------------------------------
st.set_page_config(
    page_title="EV Telemetry Predict",
    page_icon="⚡",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "ev_telemetry.db"
API_URL = "http://127.0.0.1:8000"


# ------------------------------------------------------
# Styling
# ------------------------------------------------------
st.markdown(
    """
    <style>
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }

        .main-title {
            font-size: 42px;
            font-weight: 800;
            color: #111827;
            margin-bottom: 0px;
        }

        .subtitle {
            font-size: 16px;
            color: #6B7280;
            margin-bottom: 24px;
        }

        .metric-card {
            background-color: #FFFFFF;
            border: 1px solid #E5E7EB;
            border-radius: 18px;
            padding: 18px 20px;
            box-shadow: 0px 2px 10px rgba(0,0,0,0.04);
        }

        .metric-label {
            font-size: 13px;
            color: #6B7280;
            margin-bottom: 6px;
        }

        .metric-value {
            font-size: 28px;
            font-weight: 800;
            color: #111827;
        }

        .risk-low {
            background-color: #DCFCE7;
            color: #166534;
            padding: 10px 18px;
            border-radius: 999px;
            font-weight: 800;
            display: inline-block;
        }

        .risk-medium {
            background-color: #FEF9C3;
            color: #854D0E;
            padding: 10px 18px;
            border-radius: 999px;
            font-weight: 800;
            display: inline-block;
        }

        .risk-high {
            background-color: #FEE2E2;
            color: #991B1B;
            padding: 10px 18px;
            border-radius: 999px;
            font-weight: 800;
            display: inline-block;
        }

        .section-card {
            background-color: #FFFFFF;
            border: 1px solid #E5E7EB;
            border-radius: 18px;
            padding: 20px;
            margin-top: 18px;
            box-shadow: 0px 2px 10px rgba(0,0,0,0.04);
        }

        .small-muted {
            color: #6B7280;
            font-size: 13px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ------------------------------------------------------
# Helper functions
# ------------------------------------------------------
def get_connection():
    return sqlite3.connect(DB_PATH)


def load_vins():
    if not DB_PATH.exists():
        return []

    con = get_connection()
    try:
        df = pd.read_sql("SELECT DISTINCT vin FROM raw ORDER BY vin", con)
    finally:
        con.close()

    return df["vin"].tolist()


def load_raw(vin: str, n_rows: int):
    con = get_connection()
    try:
        df = pd.read_sql(
            """
            SELECT *
            FROM raw
            WHERE vin = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            con,
            params=[vin, n_rows],
            parse_dates=["timestamp"],
        )
    finally:
        con.close()

    if df.empty:
        return df

    return df.sort_values("timestamp")


def api_is_online():
    try:
        response = requests.get(f"{API_URL}/health", timeout=2)
        return response.status_code == 200
    except requests.RequestException:
        return False


def fetch_risk_scores(vin: str, n: int):
    try:
        response = requests.post(
            f"{API_URL}/predict",
            json={"vin": vin, "n": n},
            timeout=5,
        )
        response.raise_for_status()
        return response.json().get("risk", [])
    except requests.RequestException:
        return []


def risk_level(score: float):
    if score >= 0.70:
        return "HIGH"
    if score >= 0.40:
        return "MEDIUM"
    return "LOW"


def risk_css_class(level: str):
    if level == "HIGH":
        return "risk-high"
    if level == "MEDIUM":
        return "risk-medium"
    return "risk-low"


def metric_card(label: str, value: str):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ------------------------------------------------------
# Header
# ------------------------------------------------------
st.markdown('<div class="main-title">⚡ EV Telemetry Predict</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Live vehicle telemetry, predictive maintenance scoring, and fleet health monitoring.</div>',
    unsafe_allow_html=True,
)

# refresh every 10 seconds
st_autorefresh(interval=10000, key="dashboard_refresh")


# ------------------------------------------------------
# Sidebar
# ------------------------------------------------------
st.sidebar.header("Dashboard Controls")

if api_is_online():
    st.sidebar.success("API online")
else:
    st.sidebar.error("API offline")
    st.sidebar.caption("Start it with: python -m uvicorn evtp.service:app --reload --port 8000")

vins = load_vins()

if not vins:
    st.error("No telemetry data found. Run `python test.py` or use `/simulate` from the FastAPI docs.")
    st.stop()

selected_vin = st.sidebar.selectbox("Vehicle VIN", vins)
rows_to_display = st.sidebar.slider("Rows to display", 100, 5000, 1000, 100)
rows_to_score = st.sidebar.slider("Rows to score", 1, 500, 100, 10)

if st.sidebar.button("Refresh now"):
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption("Tip: Use `/simulate` in FastAPI docs to generate more live telemetry.")


# ------------------------------------------------------
# Load data
# ------------------------------------------------------
raw = load_raw(selected_vin, rows_to_display)

if raw.empty:
    st.error(f"No data found for VIN {selected_vin}.")
    st.stop()

latest = raw.iloc[-1]

risk_scores = fetch_risk_scores(selected_vin, rows_to_score)
latest_risk = float(risk_scores[-1]) if risk_scores else 0.0
latest_level = risk_level(latest_risk)


# ------------------------------------------------------
# Top cards
# ------------------------------------------------------
top_col1, top_col2, top_col3, top_col4, top_col5 = st.columns(5)

with top_col1:
    metric_card("Speed", f"{latest['speed_kmh']:.0f} km/h")

with top_col2:
    metric_card("SOC", f"{latest['soc_pct']:.1f}%")

with top_col3:
    metric_card("Battery Temp", f"{latest['battery_temp_c']:.1f} °C")

with top_col4:
    metric_card("Inverter Temp", f"{latest['inverter_temp_c']:.1f} °C")

with top_col5:
    metric_card("Risk Score", f"{latest_risk:.2f}")

st.markdown(
    f"""
    <div style="margin-top: 20px;">
        <span class="{risk_css_class(latest_level)}">Current Risk Level: {latest_level}</span>
    </div>
    """,
    unsafe_allow_html=True,
)


# ------------------------------------------------------
# Vehicle state charts
# ------------------------------------------------------
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Vehicle State")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.caption("Speed and State of Charge")
    st.line_chart(raw.set_index("timestamp")[["speed_kmh", "soc_pct"]])

with chart_col2:
    st.caption("Thermal System")
    st.line_chart(raw.set_index("timestamp")[["battery_temp_c", "inverter_temp_c", "ambient_temp_c"]])

st.markdown("</div>", unsafe_allow_html=True)


# ------------------------------------------------------
# Risk chart
# ------------------------------------------------------
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Predictive Maintenance Risk")

if risk_scores:
    risk_df = raw.tail(len(risk_scores))[["timestamp"]].copy()

    # API returns newest-first, while chart is oldest-first
    risk_df["risk_score"] = list(reversed(risk_scores))

    st.line_chart(risk_df.set_index("timestamp")[["risk_score"]])
    st.caption("Risk score represents estimated component failure probability from engineered telemetry features.")
else:
    st.warning("No risk scores returned. Make sure the FastAPI server is running and model artifacts exist.")

st.markdown("</div>", unsafe_allow_html=True)


# ------------------------------------------------------
# Latest telemetry
# ------------------------------------------------------
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Latest Telemetry Rows")
st.dataframe(raw.tail(25), use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)