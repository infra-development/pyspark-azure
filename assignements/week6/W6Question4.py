from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, IntegerType, DoubleType

from config.config import Config
config = Config()

get_file = config.get_config()
sales_data = get_file['sales_data']

spark = SparkSession.builder.master("local[*]").appName("week6-question4").enableHiveSupport().getOrCreate()

schema="store_id integer,product string,quantity integer,revenue double"
df = spark.read.schema(schema).json(sales_data)
df.show(truncate=False)

df1 = spark.read.option("mode", "dropmalformed").schema(schema).json(sales_data)
df1.show(truncate=False)
print("Data count : " + str(df1.count()))

# df2 = spark.read.option("mode", "failfast").schema(schema).json(sales_data)
# df2.show(truncate=False)