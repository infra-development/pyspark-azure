from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("spark-sql-demo").enableHiveSupport().getOrCreate()

orders_file_path = get_config['orders_wh']

orders_df = spark.read.csv(orders_file_path, inferSchema=True, header=True)
orders_df.createOrReplaceTempView("orders")

spark.sql("create database if not exists haresh_demo")
spark.sql("show databases").show()
spark.sql("show databases").filter("namespace = 'haresh_demo'").show()
spark.sql("show databases").filter("namespace like 'haresh%'").show()

spark.sql("show tables").show()
spark.sql("use haresh_demo")
spark.sql("show tables").show()  # All the table from retail
spark.sql('create table if not exists haresh_demo.order (order_id integer, order_date string, customer_id integer, order_status string)')
spark.sql("show tables").show()

# Inserting data into order table from the view created orders (from orders_df)
spark.sql("insert into haresh_demo.order select * from orders")
spark.sql("select * from haresh_demo.order limit 5").show()
spark.sql("show tables").show()

# It's managed table
# data stored at file:/home/haresh/work/pyspark-azure/spark-warehouse/haresh_demo.db/order
spark.sql("describe extended haresh_demo.order").show(truncate=False)

spark.sql("drop table haresh_demo.order")
# spark.sql("describe extended haresh_demo.order").show(truncate=False) --> Exception (table not found)
spark.sql("show tables").show()  # table is gone, also files not present at warehouse location

# Managed Table vs External Table

#spark.sql("create table haresh_demo.orders (order_id integer, order_date string, customer_id integer, orders_status string)")
spark.sql("create table if not exists haresh_demo.new_orders (order_id integer, order_date string, customer_id integer, orders_status string) using csv location '/home/haresh/work/files/orders'")

spark.sql("select * from haresh_demo.new_orders limit 5").show()
spark.sql("describe extended haresh_demo.new_orders").show(truncate=False)

# In case of external data when you drop table --> you only drop metadata, data is not dropped.
# In case of managed table when you drop table --> you drop metadata + data

# spark.sql("truncate table haresh_demo.new_orders") # --> operation not allowed exception

# DML Operation
# Insert -> Working fine --> New file create at location /home/haresh/work/files/orders
spark.sql("insert into table haresh_demo.new_orders values (1111, '12-02-2-23', 2222, 'CLOSED')")

# In case of open source spark
# Update --> Does not work
# Delete --> Doesn't work

# In case of databricks Update and delete works


