from pyspark.sql import SparkSession

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

print("username: " + username)

spark = SparkSession.builder.master("local[*]").appName("spark-session-rdd").getOrCreate()

order_rdd = spark.sparkContext.textFile("D:/files/orders/part-00000")

collected_rdd = order_rdd.collect()

# Schema

################################################################
# order_id, date, customer_id, order_status

# 1. Orders in each category

mapped_rdd = order_rdd.map(lambda x: (x.split(",")[3], 1))

reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x+y)

reduced_sorted = reduced_rdd.sortBy(lambda x: x[1], False)

reduced_collected = reduced_sorted.collect()

# 2. Find Premium Customer

mapped_rdd = order_rdd.map(lambda x: (x.split(",")[2], 1))

reduced_rdd = mapped_rdd.reduceByKey(lambda x, y : x + y)

reduced_sorted = reduced_rdd.sortBy(lambda x : x[1], False).take(10)

# 3. Distinct Customer who placed at least 1 order

distinct_customer = order_rdd.map(lambda x: (x.split(",")[2])).distinct()

distinct_customer.count()

# 4. Customer having maximum number of CLOSED order

filtered_order = order_rdd.filter(lambda x: (x.split(",")[3] == 'CLOSED'))

mapped_rdd = filtered_order.map(lambda x: (x.split(",")[2], 1))

reduced_rdd = mapped_rdd.reduceByKey(lambda x, y : x + y)

reduced_sorted = reduced_rdd.sortBy(lambda x : x[1], False).take(10)

