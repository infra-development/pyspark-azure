
from pyspark.sql import SparkSession
from pyspark.sql.functions import column, col, expr

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("accessing-columns").enableHiveSupport().getOrCreate()

orders_path = get_config['orders']

orders_schema = 'order_id long, order_date string, cust_id long, order_status string'

orders_df = spark.read.schema(orders_schema).csv(orders_path)
orders_df.select("*").show(truncate=False)

orders_df.select("order_id", "order_date").show()
orders_df.select("order_id", orders_df.order_date, orders_df['order_date'], column('cust_id'), col('cust_id'), expr("order_status")).show(truncate=False)

orders_df.select("order_id", orders_df.order_date, orders_df['order_date'], column('cust_id'), expr('cust_id + 1 as c_id'), expr("order_status")).where(col("order_status").like("PENDING%")).show(truncate=False)