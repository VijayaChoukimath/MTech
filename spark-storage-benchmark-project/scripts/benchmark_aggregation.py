import time
from pyspark.sql.functions import col

def run_aggregation_test(df):
    start = time.time()
    df.withColumn("duration_ms", col("duration_ms").cast("double"))       .groupBy("track_genre")       .avg("duration_ms")       .count()
    end = time.time()
    return end - start