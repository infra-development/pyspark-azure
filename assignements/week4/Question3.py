from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("week4-question3").enableHiveSupport().getOrCreate()

student_review = get_config['student_review']

student_rdd = spark.sparkContext.textFile(student_review)

# 1.Find the top 20 words from Trendy tech Students Google Reviews excluding the boring words.

words = student_rdd.flatMap(lambda x: x.split(" ")).map(lambda x : x.lower())
words = words.filter(lambda x: len(x) > 2)
words = words.filter(lambda x: x.lower() not in ["a", "an", "the", "are", "is","and"])

words = words.map(lambda x: (x, 1))
words = words.reduceByKey(lambda x, y: x + y).sortBy(lambda x : x[1], ascending=False)
for word in words.take(20):
    print(word)