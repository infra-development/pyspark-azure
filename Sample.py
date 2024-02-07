from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("sample").getOrCreate()

rdd_base = spark.sparkContext.textFile("D:/files/business-employment-data.csv")
#
rdd_collected = rdd_base.collect()

for row in rdd_collected:
    print(row)