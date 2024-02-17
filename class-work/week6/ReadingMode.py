from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, LongType, DateType, StringType, StructType

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("reading-mode").enableHiveSupport().getOrCreate()

orders_sample_path = get_config['orders_sample']
orders_sample2_path = get_config['orders_sample2']
orders_sample3_path = get_config['orders_sample3']

# Reading Mode
# 1. permissive 2. failfast, 3. dropmalformed

# Allows incorrect records with null value
orders_schema = 'order_id long, order_date string, cust_id long, order_status string'
orders_sample_df = spark.read.schema(orders_schema).option("mode", "permissive").csv(orders_sample3_path)
orders_sample_df.show()

# Fails with exception for incorrect records
# orders_sample_df2 = spark.read.schema(orders_schema).option("mode", "failfast").csv(orders_sample3_path)
# orders_sample_df2.show()

# Drops incorrect records
orders_sample_df3 = spark.read.schema(orders_schema).option("mode", "dropmalformed").csv(orders_sample3_path)
orders_sample_df3.show()