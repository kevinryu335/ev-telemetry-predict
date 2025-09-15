# evtp/etl.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple
import os, sqlite3
import pandas as pd
import numpy as np

RAW_COLUMNS_REQUIRED = [
    "timestamp","vin","speed_kmh","soc_pct","battery_temp_c","motor_current_a",
    "inverter_temp_c","ambient_temp_c","tire_wear_pct","brake_wear_pct"
]

@dataclass
class ETLPipeline:
    """
    CSV -> SQLite ETL with feature engineering for EV telemetry.
    - Validates required columns & converts dtypes
    - Engineers rolling stats, deltas, and stress proxies
    - Writes 'raw' and 'features' tables to SQLite
    """
    db_path: str = "data/ev_telemetry.db"

    def __post_init__(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    # ---------- LOAD & VALIDATE ----------
    
    def load_csv(self, csv_path: str) -> pd.DataFrame:
        """Load raw csv and parse timestamp into pandas datetime (UTC-naive)."""""
        df = pd.read_csv(csv_path)
        self._validate_required_columns(df.columns.tolist())
        #parse timestamps after check
        df['timestamp'] = pd.to_datetime(df["timestamp"], errors = "raise")
        return df
    
    def _validate_required_columns(self, cols: List[str]) -> None:
        missing = [c for c in RAW_COLUMNS_REQUIRED if c not in cols]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
    
    def _coerce_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure numeric columns are numeric (non-numeric -> error)."""
        numeric_cols = [c for c in RAW_COLUMNS_REQUIRED if c not in ("timestamp","vin")]
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors="raise")
        df["vin"] = df["vin"].astype(str)
        return df
    
     # ---------- FEATURE ENGINEERING ----------

    def feature_engineer(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create per-VIN rolling means/stds & first differences (deltas),
        plus simple stress proxies used by the baseline model.
        """
        df = df.copy()
        df = self._coerce_dtypes(df)
        df = df.sort_values(["vin","timestamp"]).reset_index(drop=True)

        roll_cols = ["speed_kmh","battery_temp_c","motor_current_a","inverter_temp_c","ambient_temp_c"]
        # rolling window size ~2s @ 5Hz = 10 samples; tune as needed
        for col in roll_cols:
            # rolling mean/std per VIN
            df[f"{col}_roll_mean_10"] = (
                df.groupby("vin")[col]
                  .rolling(10, min_periods=1).mean()
                  .reset_index(level=0, drop=True)
            )
            df[f"{col}_roll_std_10"] = (
                df.groupby("vin")[col]
                  .rolling(10, min_periods=1).std()
                  .reset_index(level=0, drop=True)
                  .fillna(0.0)
            )
            # first difference (instantaneous change)
            df[f"{col}_delta"] = (
                df.groupby("vin")[col].diff().fillna(0.0)
            )

        # wear & SOC deltas (tend to be negative, but noise exists)
        for w in ["tire_wear_pct","brake_wear_pct","soc_pct"]:
            df[f"{w}_delta"] = df.groupby("vin")[w].diff().fillna(0.0)

        # stress proxies (simple, interpretable features)
        df["thermal_stress"] = (df["battery_temp_c"] + df["inverter_temp_c"]) - df["ambient_temp_c"]
        df["power_stress"]   = df["motor_current_a"] * (df["speed_kmh"].clip(lower=1))

        return df

    # ---------- PERSIST TO SQLITE ----------

    def _to_sql(self, table: str, frame: pd.DataFrame) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            frame.to_sql(table, con, if_exists="replace", index=False)
        finally:
            con.close()

    def _create_indices(self) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            cur = con.cursor()
            # helpful indices for time-range & VIN queries
            cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_vin_ts ON raw(vin, timestamp);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_feat_vin_ts ON features(vin, timestamp);")
            con.commit()
        finally:
            con.close()

    # ---------- ORCHESTRATION ----------

    def run(self, csv_path: str) -> Tuple[int, int]:
        """
        End-to-end ETL:
          1) load & validate CSV
          2) write 'raw'
          3) feature engineer
          4) write 'features'
          5) create indices
        Returns: (#raw_rows, #feature_rows)
        """
        raw = self.load_csv(csv_path)
        raw = self._coerce_dtypes(raw)
        self._to_sql("raw", raw)

        feat = self.feature_engineer(raw)
        self._to_sql("features", feat)

        self._create_indices()
        return len(raw), len(feat)