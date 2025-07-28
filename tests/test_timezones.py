import pandas as pd
import datetime as dt

# Simuliere deine DataFrame-Erstellung
df = pd.DataFrame()
df["ingest_ts"] = [dt.datetime.now(dt.timezone.utc)]

print(f"DataFrame value: {df['ingest_ts'].iloc[0]}")
print(f"DataFrame dtype: {df['ingest_ts'].dtype}")