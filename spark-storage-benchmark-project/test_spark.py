from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .config("spark.master", "local[1]") \
    .config("spark.local.dir", "C:\\tmp") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

print("Spark version:", spark.version)

spark.stop()