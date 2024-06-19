from pyspark.sql import SparkSession

spark = SparkSession \
             .builder \
             .appName("Data Engineering") \
             .master("local[3]") \
             .getOrCreate()
