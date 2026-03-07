import pandas as pd

from scripts.spark_session import create_spark
from scripts.data_loader import load_data
from scripts.benchmark_filter import run_filter_test
from scripts.benchmark_aggregation import run_aggregation_test
from scripts.storage_size import get_storage_size

from utils.config import *

spark = create_spark()

formats = {
    "Parquet":("parquet",PARQUET_PATH),
    "ORC":("orc",ORC_PATH),
    "Avro":("avro",AVRO_PATH)
}

results = []

for name,(fmt,path) in formats.items():
    df = load_data(spark,fmt,path)
    filter_time = run_filter_test(df)
    agg_time = run_aggregation_test(df)
    size = get_storage_size(path)

    results.append({
        "Format":name,
        "Storage_MB":size,
        "Filter_Time":filter_time,
        "Aggregation_Time":agg_time
    })

df = pd.DataFrame(results)
df.to_csv("results/benchmark_results.csv",index=False)

print(df)