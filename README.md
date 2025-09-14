# ev-telemetry-predict
This project simulates real-time electric vehicle telemetry (battery temperature, motor current, tire pressure, and more), ingests it into a data pipeline, and predicts component failures using machine learning. It includes a FastAPI backend for data ingestion, a scikit-learn/XGBoost model for failure prediction, and a Streamlit dashboard to visualize vehicle health and upcoming maintenance needs. This project demonstrates how predictive analytics can support proactive vehicle maintenance at scale.
#key features
Python-based data generator producing high-frequency EV sensor data

ETL pipeline (Pandas + SQLite) for cleaning, feature engineering, and storage

Failure prediction model (XGBoost) trained on synthetic labeled data

Real-time health dashboard (Streamlit) with risk scores and alerts

Designed for reliability and extensibility (Docker, CI-ready)
