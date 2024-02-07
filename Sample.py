from pyspark.sql import SparkSession
from config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("sample").getOrCreate()

orders_file_path = get_config['orders']
rdd_base = spark.sparkContext.textFile(orders_file_path)
#
rdd_collected = rdd_base.take(1)

for row in rdd_collected:
    print(row)