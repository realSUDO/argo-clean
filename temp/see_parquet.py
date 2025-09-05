import pandas as pd

df = pd.read_parquet("temp/sample.parquet")
print(df.head())         # see first 5 rows
print(df.columns)        # see column names
print(df.dtypes)         # see datatypes
print(len(df))           # number of rows

