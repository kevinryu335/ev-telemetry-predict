# ⚡ EV Telemetry Predict

### End-to-end electric vehicle telemetry simulation, predictive maintenance modeling, API serving, and live dashboard visualization.

EV Telemetry Predict is an end-to-end electric vehicle telemetry simulation and predictive maintenance project. It simulates vehicle sensor data, processes it through an ETL pipeline, stores structured telemetry in SQLite, trains a machine learning model to estimate component failure risk, serves predictions through a FastAPI backend, and visualizes vehicle health in a Streamlit dashboard.

This project was built to demonstrate a production-style workflow for automotive telemetry, predictive maintenance, and fleet health monitoring.

---

## 🚗 Project Overview

Modern electric vehicles generate large volumes of telemetry from sensors related to battery health, motor performance, thermal systems, braking, and state of charge. This project simulates that type of data and builds a full pipeline around it.

The goal is to show how vehicle telemetry can be transformed into actionable maintenance insights using:

- Data generation
- ETL and feature engineering
- Machine learning
- API-based prediction serving
- Dashboard-based monitoring

---

## ✨ Features

- Simulates electric vehicle telemetry data for one or more VINs
- Generates sensor readings such as:
  - Speed
  - State of charge
  - Battery temperature
  - Inverter temperature
  - Motor current
  - Tire wear
  - Brake wear
- Cleans and transforms telemetry data using a Pandas-based ETL pipeline
- Stores raw and engineered telemetry features in SQLite
- Trains a scikit-learn machine learning model to estimate component failure risk
- Provides FastAPI endpoints for health checks, prediction, telemetry ingestion, and simulation
- Displays live vehicle metrics, charts, and risk scores in a Streamlit dashboard
- Supports repeated simulation runs through the API for demo and testing purposes

---

## 🧱 Architecture

```text
Telemetry Generator
        ↓
ETL Pipeline
        ↓
SQLite Database
        ↓
Machine Learning Model
        ↓
FastAPI Backend
        ↓
Streamlit Dashboard
```

---

## 🛠 Tech Stack

| Category | Tools |
|---|---|
| Language | Python |
| Data Processing | Pandas, NumPy |
| Database | SQLite |
| Machine Learning | scikit-learn |
| Backend API | FastAPI, Uvicorn |
| Dashboard | Streamlit |
| Visualization | Matplotlib, Streamlit charts |
| Model Storage | joblib |

---

## 📁 Project Structure

```text
ev-telemetry-predict/
│
├── evtp/
│   ├── __init__.py
│   ├── generator.py      # Simulates EV telemetry data
│   ├── etl.py            # Cleans data and creates engineered features
│   ├── model.py          # Trains and saves the failure-risk model
│   ├── service.py        # FastAPI backend with prediction, ingestion, and simulation routes
│   └── dashboard.py      # Streamlit dashboard
│
├── data/                 # Local generated data, ignored by Git
├── models/               # Local trained model artifacts, ignored by Git
├── test.py               # Runs generator, ETL, and model training
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup

### 1. Clone the repository

```bash
git clone https://github.com/kevinryu335/ev-telemetry-predict.git
cd ev-telemetry-predict
```

### 2. Create and activate a virtual environment

For Git Bash on Windows:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

For PowerShell on Windows:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## ▶️ Run the Full Pipeline

To generate sample telemetry data, run the ETL pipeline, and train the model:

```bash
python test.py
```

This creates local generated files:

```text
data/raw.csv
data/ev_telemetry.db
models/model.joblib
models/feature_cols.json
```

These files are generated locally and should not be committed to GitHub.

---

## 🌐 Start the FastAPI Server

Run:

```bash
python -m uvicorn evtp.service:app --reload --port 8000
```

Then open the API docs in your browser:

```text
http://127.0.0.1:8000/docs
```

Available endpoints:

```text
GET  /health
POST /predict
POST /ingest
POST /simulate
```

---

## 🔌 API Usage

### Health Check

```bash
curl http://127.0.0.1:8000/health
```

Expected response:

```json
{
  "status": "ok"
}
```

---

### Predict Failure Risk

Use the `/predict` endpoint with a VIN and number of recent rows to score.

Example request:

```json
{
  "vin": "EV001",
  "n": 50
}
```

Example response:

```json
{
  "vin": "EV001",
  "n": 50,
  "risk": [0.12, 0.18, 0.23]
}
```

---

### Ingest Telemetry

Use the `/ingest` endpoint to add one or more telemetry records to the database and recompute features.

Example request:

```json
{
  "records": [
    {
      "timestamp": "2026-02-23T12:00:00",
      "vin": "EV001",
      "speed_kmh": 55,
      "soc_pct": 82.5,
      "battery_temp_c": 33.2,
      "motor_current_a": 140,
      "inverter_temp_c": 42.1,
      "ambient_temp_c": 18.0,
      "tire_wear_pct": 99.8,
      "brake_wear_pct": 99.9
    }
  ],
  "recompute_features": true
}
```

---

### Simulate New Telemetry

Use the `/simulate` endpoint in the FastAPI docs to generate and ingest new telemetry automatically.

Example inputs:

```text
vin = EV777
steps = 300
```

Example response:

```json
{
  "simulated_rows": 300,
  "vin": "EV777",
  "inserted_rows": 300,
  "vins": ["EV777"],
  "recomputed_features": true
}
```

---

## 📊 Start the Dashboard

Keep the FastAPI server running in one terminal.

In a second terminal, run:

```bash
python -m streamlit run evtp/dashboard.py
```

The dashboard displays:

- API status
- VIN selector
- Speed
- State of charge
- Battery temperature
- Inverter temperature
- Failure-risk score
- Vehicle telemetry charts
- Latest telemetry rows

To see new simulated data, open the FastAPI docs, run `/simulate`, and refresh or wait for the dashboard to auto-refresh.

---

## 🧪 Notes on the Data

The telemetry data is synthetic and generated for demonstration purposes.

The failure-risk labels are also synthetic and are based on engineered stress and wear features.

This project is intended to show the structure of an automotive telemetry and predictive maintenance system, not to make real-world vehicle safety predictions.

---

## 🎯 What This Project Demonstrates

This project demonstrates experience with:

- Building modular Python applications
- Designing ETL pipelines
- Working with time-series telemetry data
- Training machine learning models
- Serving ML predictions through an API
- Building interactive dashboards
- Structuring a project for real-world engineering workflows

---

## 🚀 Future Improvements

- Add real tire pressure simulation
- Add anomaly detection for sudden telemetry changes
- Add Docker support
- Add GitHub Actions for automated tests
- Add WebSocket streaming for real-time telemetry updates
- Add dashboard screenshots to the README
- Improve model evaluation and risk calibration

---

## ⚠️ Disclaimer

This project uses synthetic vehicle telemetry and synthetic failure labels. It is designed as a portfolio project to demonstrate backend, data engineering, machine learning, and dashboard development skills in an automotive context.
