import pandas as pd

parquet_file = "2025-07-08.parquet"
p_df = pd.read_parquet(parquet_file)
p_df.to_csv("output.csv", index=False)
