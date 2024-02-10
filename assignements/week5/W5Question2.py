from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, IntegerType, DoubleType

from config.config import Config
config = Config()

get_file = config.get_config()
products_file = get_file['products']

spark = SparkSession.builder.master("local[*]").appName("week5-question2").enableHiveSupport().getOrCreate()
schema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("Category", IntegerType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("ImageURL", StringType(), True)
])
products_df = spark.read.csv(products_file, header=True, schema=schema)
products_df.printSchema()
products_df.createOrReplaceTempView("product_temp")
products_df.show()

# 2.1 Find total number of products in the given datasets
print(products_df.count())
spark.sql("select count(*) from product_temp").show(truncate=False)

# 2.2 Find the number of unique categories of products in the given dataset
unique_category = products_df.select("Category").distinct().count()
print("Unique category : --> " + str(unique_category))
spark.sql("select count(distinct category) from product_temp").show()

# 2.3 Find the top 5 most expensive products based on their price, along with their product name, category,
# and image URL.

products_df.select("ProductName", "Category", "Price", "ImageURL").orderBy("Price", ascending=False).limit(5).show()
spark.sql("select productname, category, price, imageurl from product_temp order by price desc limit 5").show()

# 2.4. Find the number of products in each category that have a price greater than $100. Display the results in a
# tabular format that shows the category name and the number of products that satisfy the condition

products_df.where("price > 100").groupBy("category").count().withColumnRenamed("count", "number_of_products").show()
spark.sql("select category, count(*) as number_of_products from product_temp where price > 100 group by category").show()

# 2.5. What are the product names and prices of products that have a price greater than $200 and belong to category 5?

products_df.where("price > 200 and category = 5").select("productname", "price").show()
spark.sql("select productname, price from product_temp where price > 200 and category = 5").show()

