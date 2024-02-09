from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("spark-sql-demo").enableHiveSupport().getOrCreate()

orders_file_path = get_config['orders_wh']


orders_df = spark.read.csv(orders_file_path, inferSchema=True, header=True)
orders_df.createOrReplaceTempView("orders")


# 1. Top 15 customers who placed the most number of orders
orders_df.groupBy("customer_id").count().sort("count", ascending = False).limit(15).show()
spark.sql("select customer_id, count(order_id) as count from orders group by customer_id order by count desc limit 15").show()

# 2. Find the number of orders under each order status
orders_df.groupBy("order_status").count().show()
spark.sql("select order_status, count(order_id) as count from orders group by order_status order by count desc").show()

# 3. Find the number of active customers
print(orders_df.select("customer_id").distinct().count())
spark.sql("select count(distinct customer_id) from orders").show()

# count with group by is transformation otherwise it's an action

# 4. Customer with most number of closed orders
orders_df.filter("order_status = 'CLOSED'").groupBy("customer_id").count().sort("count", ascending=False).withColumnRenamed("count", "cnt").show()
spark.sql("select customer_id, count(*) cnt from orders where order_status = 'CLOSED' group by customer_id sort by cnt desc").show()

# order by --> Transformation
# filter --> Transformation
# show --> action
# head --> action
# tail --> action
# take --> action
# collection --> action
# distinct --> transformation
# join --> transformation
# printSchema --> Utility function (Neither transformation nor actions)
# cache --> Utility function
# createOrReplaceTempView --> Utility function

