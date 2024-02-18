from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, IntegerType, DoubleType

from config.config import Config
config = Config()

get_file = config.get_config()
train = get_file['train']

spark = SparkSession.builder.master("local[*]").appName("week6-question3").enableHiveSupport().getOrCreate()

df = spark.read.option("header", "True").csv(train, inferSchema=True)
df.show(5, truncate=False)
df.printSchema()

# Drop the columns passenger_name ange age from the dataset
new_df = df.drop("passenger_name", "age")
new_df.show(5)

# Count the number of rows after removing duplicates of columns train_number and ticket_number
duplicate_removed = df.dropDuplicates(["train_number", "ticket_number"])
print(duplicate_removed.count())

uniq_trains = df.select("train_name").distinct()
print("unique_trains : "+str(uniq_trains.count()))