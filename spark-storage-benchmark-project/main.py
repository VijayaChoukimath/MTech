import pandas as pd
import time
import os

os.environ['TZDIR'] = r'C:\Users\Vijaya Choukimath\Desktop\MTech\Projects\Module3\.venv\Lib\site-packages\tzdata\zoneinfo'

from utils.config import *

formats = {
    "Parquet":("parquet",PARQUET_PATH),
    "ORC":("orc",ORC_PATH),
    "Avro":("avro",AVRO_PATH)
}

results = []

for name,(fmt,path) in formats.items():
    if fmt == "parquet":
        df = pd.read_parquet(os.path.join(path, "data.parquet"))
    elif fmt == "orc":
        df = pd.read_orc(os.path.join(path, "data.orc"))
    elif fmt == "avro":
        from fastavro import reader
        with open(os.path.join(path, "data.avro"), 'rb') as f:
            avro_reader = reader(f)
            records = list(avro_reader)
        df = pd.DataFrame(records)

    # Filter test
    start = time.time()
    filtered = df[df['popularity'] > 50]
    filter_time = time.time() - start

    # Aggregation test
    start = time.time()
    agg = df.groupby('track_genre')['popularity'].mean()
    agg_time = time.time() - start

    # Storage size
    size = os.path.getsize(os.path.join(path, f"data.{fmt}")) / (1024 * 1024)  # MB

    results.append({
        "Format":name,
        "Storage_MB":size,
        "Filter_Time":filter_time,
        "Aggregation_Time":agg_time
    })

df = pd.DataFrame(results)
df.to_csv("data/output/benchmark_results.csv",index=False)

print(df)