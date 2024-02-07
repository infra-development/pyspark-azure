from pyspark.sql import SparkSession
from config import Config

import getpass
username = getpass.getuser()

# spark1 = (SparkSession
#          .builder
#          .config("spark.ui.port", '0')
#          .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
#          .enableHiveSupport()
#          .master('yarn')
#          .getOrCreate()
#          )
config = Config()
get_config = config.get_config()
spark = SparkSession.builder.master("local[*]").appName("spark-session-rdd").getOrCreate()

orders_path = get_config['orders']
order_rdd = spark.sparkContext.textFile(orders_path)

# Schema

################################################################
# order_id, date, customer_id, order_status

# 1. Orders in each category

mapped_rdd = order_rdd.map(lambda x: (x.split(",")[3], 1))

reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x+y)

reduced_sorted = reduced_rdd.sortBy(lambda x: x[1], False)

collected = reduced_sorted.take(3)

for row in collected:
    print(row)

# 2. Find Premium Customer

customers_rdd = order_rdd.map(lambda x: (x.split(",")[2], 1))

reduced_customer = customers_rdd.reduceByKey(lambda x, y : x + y)

sorted_customer = reduced_customer.sortBy(lambda x : x[1], False).take(10)

for row in sorted_customer:
    print(row)

# # 3. Distinct Customer who placed at least 1 order

distinct_customer = order_rdd.map(lambda x: (x.split(",")[2])).distinct()

print(distinct_customer.count())

# # 4. Customer having maximum number of CLOSED order

filtered_order = order_rdd.filter(lambda x: (x.split(",")[3] == 'CLOSED'))

filtered_customer = filtered_order.map(lambda x: (x.split(",")[2], 1))

customer_sorted = filtered_customer.reduceByKey(lambda x, y : x + y)

reduced_sorted = customer_sorted.sortBy(lambda x : x[1], False).take(10)

for row in reduced_sorted:
    print(row)

