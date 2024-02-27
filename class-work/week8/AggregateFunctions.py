from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import column, col, expr, count, countDistinct, avg, sum
from pyspark.sql.types import IntegerType

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("accessing-columns").enableHiveSupport().getOrCreate()

orders_path = get_config['order_data']
window_path = get_config['windowdata']
window_modified_path = get_config['windowdata_modified']


orders_df = spark.read.csv(orders_path, inferSchema=True, header=True)
window_df = spark.read.csv(window_path, inferSchema=True, header=True)
window_modified_df = spark.read.csv(window_modified_path, inferSchema=True, header=True)

orders_df.show()
orders_df.printSchema()
orders_df.withColumn("InvoiceNo", orders_df.InvoiceNo.cast(IntegerType()))

# 1. Simple aggregations --> will give only one output row, like count total records, sum of quantity
# 2. Grouping Aggregation --> will do group by
# 3. Windowing functions -->

print(orders_df.count())
orders_df.select(count("*").alias("row_count"),
                 countDistinct("InvoiceNo").alias("unique_invoice"),
                 sum("quantity").alias("total_quantity"),
                 avg("unitprice").alias("avg_price")).show()

orders_df.selectExpr("count(*) as row_count",
                 "count(distinct invoiceno) as unique_invoice",
                 "sum(quantity) as total_quantity",
                 "avg(unitprice) as avg_price").show()

orders_df.createOrReplaceTempView("orders_view")

# Count(*) will consider null, count(on specific column) will ignore null
spark.sql("select count(*) as row_count, count(distinct invoiceno) as unqiue_invoice, sum(quantity) as total_quantity, avg(unitprice) as avg_price from orders_view").show()


# Grouping Aggregation
# Total quantity for invoice number, and total sum

orders_df.groupBy("country", "invoiceno").agg(sum("quantity").alias("total_quantity"), sum(expr("quantity * unitprice")).alias("invoice_value")).sort("invoiceno").show()

orders_df.groupBy("country", "invoiceno").agg(expr("sum(quantity) as total_quantity"), expr("sum(quantity * unitprice) as invoice_value")).sort("invoiceno").show()



