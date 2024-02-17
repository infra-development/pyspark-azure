from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, LongType, DateType, StringType, StructType, DoubleType, IntegerType

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("schema-enforcement").enableHiveSupport().getOrCreate()

orders_sample_path = get_config['orders_sample']
orders_sample2_path = get_config['orders_sample2']
customer_nested_path = get_config['customer_nested']
order_items = get_config['order_items']

orders_schema = 'order_id long, order_date string, cust_id long, order_status string'

orders_sample_df = spark.read.schema(orders_schema).csv(orders_sample_path)
orders_sample_df.createOrReplaceTempView("orders_view")

# spark.read
# spark.sql
# spark.table
# spark.range
# snmpark.createDataFrame --> create dataframe from list
# spark.sparkContext.parallelize(list) --> creating rdd

# Apart from the ways we have seen so far few more ways to create dataframe
another_df = spark.table("orders_view")
another_df.show()

spark.range(5).show()  # --> Single column dataframe --> column name = id
spark.range(0, 8, 2).show()  # --> Single column dataframe --> column name = id

list = [
    (1, '2013-07-25 00:00:00.0', 11599, 'CLOSED'),
    (2, '2013-07-26 00:00:00.0', 11699, 'PENDING_PAYMENT'),
    (3, '2013-07-27 00:00:00.0', 11799, 'COMPLETE')
]

listDF = spark.createDataFrame(list).toDF('order_id', 'order_date', 'cust_id',
                                          'order_status')  # --> passing column names to toDF function
listDF.show()

orders_schema1 = 'order_id long, order_date string, cust_id long, order_status string'
orders_schema2 = ['order_id', 'order_date', 'cust_id', 'order_status']
listDF1 = spark.createDataFrame(list, orders_schema1)  # --> Passing schema along with list
listDF2 = spark.createDataFrame(list, orders_schema2)  # --> Passing schema along with list
listDF1.show()
listDF2.show()

# Instead of passing list we can pass rdd and create dataframe using createDataFrame
orders_sample_rdd = spark.sparkContext.textFile(orders_sample_path)
convert_to_type_rdd = orders_sample_rdd.map(
    lambda x: (int(x.split(",")[0]), str(x.split(",")[1]), int(x.split(",")[2]), str(x.split(",")[3])))
final_df = convert_to_type_rdd.take(5)

listDF3 = spark.createDataFrame(final_df, orders_schema1)
listDF3.show()

# Another way of creating dataframe
type_rdd = convert_to_type_rdd.toDF(orders_schema1)
type_rdd.show()

# Nested Schema
ddlSchema = "customer_id long, fullname struct<firstname:string, lastname:string>, city string"
df_nested = spark.read.format("json").schema(ddlSchema).load(customer_nested_path)
df_nested.show()

customer_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("fullname", StructType([
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("city", StringType(), True)
])

df_nested2 = spark.read.format("json").schema(customer_schema).load(customer_nested_path)
df_nested2.show()

# Add new column --> withColumn
# Rename --> withColumnRenamed
# Drop column --> drop

df_row = spark.read.csv(order_items)
df_row.show()
df_row.printSchema()

df_final = df_row.toDF("order_item_id", "order_id", "product_id", "quantity", "subtotal", "product_price")
df_final.show()

df1 = df_final.drop("subtotal")
df1.show()

# df1.select("*", "product_price * quantity as subtotal").show() # --> Will not work because this is expression
df1.select("*", expr("product_price * quantity as subtotal")).show()  # --> Will work
df1.selectExpr("*", "product_price * quantity as subtotal").show()  # --> Will work

products = get_config['products']
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_category", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_description", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("product_image", StringType(), True)
])
products_df = spark.read.schema(schema).csv(products)

products_df.show()

new_product_df = products_df.withColumn("product_price", expr(
    "CASE WHEN product_name like '%Nike%' THEN product_price * 1.2 WHEN product_name like '%Armour%' THEN product_price * 1.1 ELSE product_price END"))

new_product_df.show()

# How to remove duplicate record from dataframe
mylist = [(1, "Kapil", 34),
          (1, "Kapil", 34),
          (1, "Satish", 26),
          (2, "Satish", 26)]

myListDf = spark.createDataFrame(mylist).toDF("id", "name", "age")
myListDf.show()

df1 = myListDf.distinct()  # distinct will work on all the columns it will check all the columns until passed specific
df1.show()

df2 = myListDf.dropDuplicates(["name", "age"])
df2.show()

df3 = myListDf.dropDuplicates(["id"])
df3.show()

# Spark Session
# It's entry point to spark cluster
# Used for High level API Dataframe, Spark SQL
# Instead of having sparkContext, hiveContext, sqlContext --> Everything is now encapsulated in single spark session

# We need spark context when dealing at RDD level
spark2 = spark.newSession() # --> for another session

# Both have different namespaces

