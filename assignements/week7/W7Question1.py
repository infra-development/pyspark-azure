from time import sleep

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, IntegerType, DoubleType

from config.config import Config
config = Config()

get_file = config.get_config()
customers_path = get_file['customer_transfer']

spark = SparkSession.builder.master("local[*]").appName("week7-question1").enableHiveSupport().getOrCreate()


# A)Design a caching mechanism using dataframes to enhance the performance of data retrieval for the following use cases

# A.1) Your marketing team wants to identify the top-selling products
# based on revenue for a given time period. The query is expected to be
# executed frequently, and the results need to be returned quickly. Design a
# caching strategy that efficiently retrieves the top-selling products by
# revenue.

# Additionally, demonstrate the impact of caching by comparing the retrieval
# time for Top 10 best-selling products from start_date = "2023-05-01" to
# end_date = "2023-06-08" before and after implementing the caching strategy.
# [Note : Strategize your caching in such a way that the right Dataframes are
# cached at the right time for maximal performance gains]

cust_schema = 'customer_id long, purchase_date date, product_id integer, transaction_amount double'
customer_df = spark.read.schema(cust_schema).csv(customers_path)
#print(customer_df.count())

filtered_df = customer_df.filter("purchase_date > '2023-05-01' and purchase_date < '2023-06-08'")
#print(filtered_df.count())

final_df = filtered_df.groupBy("product_id").sum("transaction_amount").withColumnRenamed("sum(transaction_amount)", "revenue").sort("revenue", ascending=False).withColumn("revenue", format_number("revenue", 3))
final_df.show()



# A.2) Find the top 10 customers with maximum transaction amount for the
# same date range of start_date = "2023-05-01" to end_date = "2023-06-08"

(filtered_df.groupBy("customer_id").sum("transaction_amount")
 .withColumnRenamed("sum(transaction_amount)", "transactions")
 .sort("transactions", ascending=False)
 .withColumn("transactions", format_number("transactions", 3))
 .show())

# A.3) Implement all above using spark table
customer_df.createOrReplaceTempView("customer_tmp")

spark.sql("select product_id, sum(transaction_amount) as revenue from customer_tmp where purchase_date >= '2023-05-01' and purchase_date < '2023-06-08' group by product_id order by revenue desc limit 10").show()

spark.sql("select customer_id, sum(transaction_amount) as revenue from customer_tmp where purchase_date >= '2023-05-01' and purchase_date < '2023-06-08' group by customer_id order by revenue desc limit 10").show()

# A. 4 Find the top 10 regular customers (having at least one purchase in any month) who are eligible for special
# offer. Also, demonstrate the performance gains achieved by using persist.
new_df = customer_df.withColumn("purchase_year", year("purchase_date")).withColumn("purchase_month", month("purchase_date"))
cust_months_count = new_df.groupBy("customer_id", "purchase_year", "purchase_month").agg(countDistinct("purchase_month").alias("distinct_months"))
regular_customers = cust_months_count.groupBy("customer_id").count().orderBy("count", ascending=False).limit(20).show()


# A.5 Using Cache
cust_months_count = new_df.groupBy("customer_id", "purchase_year", "purchase_month").agg(countDistinct("purchase_month").alias("distinct_months")).cache()
cust_months_count.groupBy("customer_id").count().orderBy("count", ascending=False).limit(20).show()

# A.6 Using persist
cust_months_count = new_df.groupBy("customer_id", "purchase_year", "purchase_month").agg(countDistinct("purchase_month").alias("distinct_months")).persist(StorageLevel.MEMORY_AND_DISK)
cust_months_count.groupBy("customer_id").count().orderBy("count", ascending=False).limit(20).show()

# sleep(300)