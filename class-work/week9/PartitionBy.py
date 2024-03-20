from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StructField

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("writing-dataframe").enableHiveSupport().getOrCreate()

orders_path = get_config['orders']
customers_path = get_config['customers']

order_schema = "order_id string,order_date date, customer_id long, order_status string"
orders_df = spark.read.format("csv").schema(order_schema).load(orders_path)

schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("cust_fname", StringType(), True),
    StructField("cust_lname", StringType(), True),
    StructField("cust_email", StringType(), True),
    StructField("cust_password", StringType(), True),
    StructField("cust_street", StringType(), True),
    StructField("cust_city", StringType(), True),
    StructField("cust_state", StringType(), True),
    StructField("cust_zipcode", IntegerType(), True)
])
customers_df = spark.read.format("csv").schema(schema).load(customers_path)
orders_df.show()


orders_df.write.format("csv").mode("overwrite").option("path", "/home/haresh/work/warehouse/orders").save()
orders_df1 = spark.read.format("csv").schema(order_schema).load("/home/haresh/work/warehouse/orders")
orders_df1.show()

orders_df1.createOrReplaceTempView("orders")

orders_df1.write.format("csv").mode("overwrite").partitionBy("order_status").option("path",
                                                                                    "/home/haresh/work/warehouse/orders-partitioned").save()
orders_df2 = spark.read.format("csv").schema(order_schema).load("/home/haresh/work/warehouse/orders-partitioned")

orders_df2.createOrReplaceTempView("orders_temp")
spark.sql("select * from orders_temp where order_status = 'COMPLETE'").show()
# Only order_status=COMPLETE folder will be scanned, remaining won't be scanned
# So try to filter based on partitioned column
# numPartition = 9 as 1 GB file 128 mb of each partition then total 9 partitions
# if 9 order_status then 9 partition will be created and - 9 files inside these 9 partitioned folders as we have

spark.sql("select count(*) from orders_temp where customer_id = '8827'").show()
# Here we won't get optimization need to filter based on partitioned column

customers_df.write.format("csv").mode("overwrite").partitionBy("cust_state", "cust_city").option("path",
                                                                                    "/home/haresh/work/warehouse/customers-partitioned").save()
# outer folders will be cust_state=PR and inner folder will be cust_city=Caguas

cust_df_new = spark.read.schema(schema).csv("/home/haresh/work/warehouse/customers-partitioned")
cust_df_new.show()

cust_df_new.createOrReplaceTempView("cust_df_new")
spark.sql("select count(*) from cust_df_new where cust_state = 'PR' and cust_city = 'Caguas'").show()
# Here only one folder will be scanned
