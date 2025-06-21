import pandas as pd

df = pd.read_parquet('indexes/system_logs/error_tracking/error1.parquet')
print(df.columns)
