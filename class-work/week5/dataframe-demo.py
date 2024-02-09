from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("dataframe-demo").getOrCreate()

orders_file_path = get_config['orders_wh']

orders_df = (spark
             .read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(orders_file_path))

# inferSchema is not advised as it might not infer correctly sometimes,
# and it takes time to infer as spark has to scan the data in order to determine the data type

orders_df_demo = spark.read.csv(orders_file_path)

# Same way
# orders_df_demo2 = spark.read.json(orders_file_path) --> Column name and data are embedded
# orders_df_demo3 = spark.read.parquet(orders_file_path) --> Column name and data are embedded, column based file format works very well with spark
# orders_df_demo4 = spark.read.orc(orders_file_path) --> Column name and data are embedded
# spark.read.jdbc
# spark.read.table

orders_df.show(1)
orders_df.printSchema()

# Rename the order_status to status
renamed_status = orders_df.withColumnRenamed("order_status", "status")
renamed_status.show(1)

# Create new column from existing column and applying function on it
new_column = renamed_status.withColumn("order_date_new", to_timestamp("order_date"))
new_column.show(1)

filtered_df = orders_df.where("customer_id = 11599")
filtered_df.show(truncate=False)

# Another way
filtered_df = orders_df.filter("customer_id = 11599")
filtered_df.show(truncate=False)

# Create dataframe into sql table
orders_df.createOrReplaceTempView("orders")
filtered_sql_df = spark.sql("select * from orders where order_status = 'CLOSED'")
filtered_sql_df.show(truncate=False)

# Convert sql table to dataframe
orders_dataframe = spark.read.table("orders")