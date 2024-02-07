from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("pyspark-azure").getOrCreate()

df = spark.read.csv("D:/files/business-employment-data.csv")

df.show()
