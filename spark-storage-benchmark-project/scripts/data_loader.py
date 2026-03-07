def load_data(spark, format_name, path):
    return spark.read.format(format_name).load(path)