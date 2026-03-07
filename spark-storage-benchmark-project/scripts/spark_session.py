from pyspark.sql import SparkSession

def create_spark():
    spark = SparkSession.builder         .appName("StorageBenchmark")         .config("spark.jars.packages","org.apache.spark:spark-avro_2.12:3.5.1")         .getOrCreate()
    return spark