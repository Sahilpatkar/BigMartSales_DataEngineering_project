from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder \
    .appName("Sample PySpark Application") \
    .getOrCreate()

print(spark)


# Stop the Spark session
spark.stop()
