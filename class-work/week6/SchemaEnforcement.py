from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, LongType, DateType, StringType, StructType

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("schema-enforcement").enableHiveSupport().getOrCreate()

orders_sample_path = get_config['orders_sample']
orders_sample2_path = get_config['orders_sample2']

# samplingRation 0.1 scans 10% of total data to inferSchema
# samplingRation 1 scans 100% of total data to inferSchema
orders_df = spark.read.csv(orders_sample_path, inferSchema=True, samplingRatio=0.1, header=True)

orders_schema = 'order_id long, order_date date, cust_id long, order_status string'
orders_sample_df = spark.read.schema(orders_schema).csv(orders_sample_path)
orders_sample_df.show()
orders_sample_df.printSchema()

# If datatype miss match in that case all the values of that column will be null
structSchema = StructType([
    StructField("order_id", LongType()),
    StructField("order_date", DateType()),
    StructField("cust_id", LongType()),
    StructField("order_status", StringType())
])

orders_sample_df2 = spark.read.schema(structSchema).csv(orders_sample_path)
orders_sample_df2.show()
orders_sample_df2.printSchema()

# Giving dateFormat if data contains another date format
orders_schema2 = 'order_id long, order_date date, cust_id long, order_status string'
orders_sample2_df = spark.read.schema(orders_schema2).option("dateFormat", "mm-dd-yyyy").csv(orders_sample2_path)
orders_sample2_df.show()
orders_sample2_df.printSchema()

# Load date as string and then apply str ---> date
orders_schema3 = 'order_id long, order_date string, cust_id long, order_status string'
orders_sample3_df = spark.read.schema(orders_schema3).csv(orders_sample2_path)
orders_sample3_df.printSchema()

# Create new column
new_df = orders_sample3_df.withColumn("order_date_new", to_date("order_date", "mm-dd-yyyy"))
new_df.printSchema()

# Change existing column to date
new_df2 = orders_sample3_df.withColumn("order_date", to_date("order_date", "mm-dd-yyyy"))
new_df2.printSchema()