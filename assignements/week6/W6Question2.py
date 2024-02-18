from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, IntegerType, DoubleType

from config.config import Config
config = Config()

get_file = config.get_config()
library_data = get_file['library_data']

spark = SparkSession.builder.master("local[*]").appName("week6-question2").enableHiveSupport().getOrCreate()

schema = StructType([
    StructField("library_name", StringType()),
    StructField("location", StringType()),
    StructField("books", ArrayType(
        StructType([
            StructField("book_id", StringType()),
            StructField("book_name", StringType()),
            StructField("author", StringType()),
            StructField("copies_available", IntegerType())
        ])
    )),
    StructField("members", ArrayType(
        StructType([
            StructField("member_id", StringType()),
            StructField("member_name", StringType()),
            StructField("age", IntegerType()),
            StructField("books_borrowed", ArrayType(StringType()))
        ])

    ))
])

df = spark.read.schema(schema).json(library_data)
df.show(5, truncate=False)
df.printSchema()