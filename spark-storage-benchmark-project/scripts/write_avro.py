from pyspark.sql import SparkSession
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
input_path = os.path.join(project_root, "data", "input", "spotify-tracks-dataset.csv")
output_path = os.path.join(project_root, "data", "output", "avro")

spark = SparkSession.builder \
    .appName("Spotify Avro Writer") \
    .config("spark.master", "local[1]") \
    .config("spark.local.dir", "C:\\tmp") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

df = df.drop(df.columns[0])

start = time.time()

try:
    df.write.format("avro").mode("overwrite").save(output_path)
except Exception as e:
    print(f"Error writing Avro: {e}")

end = time.time()

print("Avro write time:", end-start)

spark.stop()