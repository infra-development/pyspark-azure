from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, IntegerType, DoubleType

from config.config import Config
config = Config()

get_file = config.get_config()
customer_files = get_file['customers']

spark = SparkSession.builder.master("local[*]").appName("week6-question1").enableHiveSupport().getOrCreate()

list = [("Spring", 12.3), ("Summer", 10.5), ("Autumn", 8.2), ("Winter", 15.1)]
df = spark.createDataFrame(list).toDF("season", "windspeed")
df.show()