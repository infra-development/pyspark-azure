from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("writing-dataframe").enableHiveSupport().getOrCreate()

orders_path = get_config['orders']

order_schema = "order_id string,order_date date, customer_id long, order_status string"
orders_df = spark.read.format("csv").schema(order_schema).load(orders_path)
orders_df.show()

print(orders_df.rdd.getNumPartitions())

# 4 Modes to write
# overwrite
# ignore
# append
# errorIfExists

orders_df.write.format("csv").mode("overwrite").option("path", "/home/haresh/work/warehouse/orders").save()
orders_df.write.format("avro").mode("append").option("path", "/home/haresh/work/warehouse/orders").save()
orders_df.write.format("orc").mode("append").option("path", "/home/haresh/work/warehouse/orders").save()

# No of part files will be equal to the number of partitions
# If 9 partitions then 9 files will be there

# If no format is specified then by default it will be parquet
orders_df.write.mode("overwrite").option("path", "/home/haresh/work/warehouse/orders").save()
