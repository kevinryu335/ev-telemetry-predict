from evtp.generator import TelemetryGenerator
from evtp.etl import ETLPipeline

# 1) generate a tiny dataset
TelemetryGenerator(["EV001","EV002"], hz=5.0).to_csv("data/raw.csv", rows=1200)

# 2) run ETL
rows_raw, rows_feat = ETLPipeline().run("data/raw.csv")
print(rows_raw, rows_feat)  # should be equal; features adds columns, not rows