from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("accessing-columns").enableHiveSupport().getOrCreate()

window_path = get_config['windowdata']
window_modified_path = get_config['windowdata_modified']


window_df = spark.read.csv(window_path, inferSchema=True, header=True)
window_modified_df = spark.read.csv(window_modified_path, inferSchema=True, header=True)

# Windowing Aggregation

window_df.sort("country").show()
# 1. partition by based on country,
# 2. sort based on week number
# 3, define window size

myWindow = Window.partitionBy("country").orderBy("weeknum").rowsBetween(Window.unboundedPreceding, Window.currentRow)
myWindow1 = Window.partitionBy("country").orderBy("weeknum").rowsBetween(-2, Window.currentRow) # Current row and 2 previous rows
result_df = window_df.withColumn("running_total", sum("invoicevalue").over(myWindow))
result_df.show()

window_modified_df.orderBy("country", "invoicevalue").show()
anotherWindow = Window.partitionBy("country").orderBy("weeknum").rowsBetween(Window.unboundedPreceding, Window.currentRow)
result_df = window_modified_df.withColumn("running_total", sum("invoicevalue").over(anotherWindow))
result_df.show()

rank_window = Window.partitionBy("country").orderBy(desc("invoicevalue"))
rank_result_df = window_modified_df.withColumn("rank", rank().over(rank_window))
rank_result_df.show()

row_num_window = Window.partitionBy("country").orderBy(desc("invoicevalue"))
row_num_result_df = window_modified_df.withColumn("row_num", row_number().over(row_num_window))
row_num_result_df.show()

dense_rank_window = Window.partitionBy("country").orderBy(desc("invoicevalue"))
dense_rank_result_df = window_modified_df.withColumn("dense_rank", dense_rank().over(rank_window))
dense_rank_result_df.show()

dense_rank_result_df.select("*").where("dense_rank = 1").drop("dense_rank").show() # Select only first

lag_window = Window.partitionBy("country").orderBy("weeknum")
lag_window_df = window_modified_df.withColumn("previous_week", lag("invoicevalue").over(lag_window))
lag_final = lag_window_df.withColumn("invoice_diff", expr("invoicevalue - previous_week"))
lag_final.show()

lead_window = Window.partitionBy("country").orderBy("weeknum")
lead_window_df = window_modified_df.withColumn("next_week", lead("invoicevalue").over(lead_window))
lead_window_df.show()


another_window = Window.partitionBy("country")
another_window_df = window_modified_df.withColumn("total_invoice", sum("invoicevalue").over(another_window))
another_window_df.show()

