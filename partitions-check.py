from pyspark.sql import SparkSession

import getpass
username = getpass.getuser()

spark = (SparkSession
         .builder
         .config("spark.ui.port", '0')
         .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
         .enableHiveSupport()
         .master('yarn')
         .getOrCreate()
         )


order_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")

order_rdd.getNumPartitions()  # defaultMinPartitions set to 2

spark.sparkContext.defaultMinPartitions()

spark.SparkContext.defaultParallelism # no.of default tasks that can run in parallel

# countByValue --> map + reduceByKey
# countByValue --> Action


# map, filter, flatMap --> Narrow Transformation

# reduceByKey, groupByKey --> Wide Transformation

# spark.sparkContext.broadcast(customers.collect())

# Repartition Vs Coalesce

repartitioned_rdd = order_rdd.repartition(6)

