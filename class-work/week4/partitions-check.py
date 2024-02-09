from pyspark.sql import SparkSession
from config.config import Config

import getpass
username = getpass.getuser()

# spark = (SparkSession
#          .builder
#          .config("spark.ui.port", '0')
#          .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
#          .enableHiveSupport()
#          .master('yarn')
#          .getOrCreate()
#          )

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("partitions-check").getOrCreate()

order_rdd = spark.sparkContext.textFile(get_config['orders'])

print("getNumPartitions : " + str(order_rdd.getNumPartitions())) # defaultMinPartitions set to 2

print("defaultMinPartitions : " + str(spark.sparkContext.defaultMinPartitions))


# no.of default tasks that can run in parallel
# Mainly number of cores available
print("defaultParallelism : " + str(spark.sparkContext.defaultParallelism))

# countByValue --> map + reduceByKey
# countByValue --> Action


# map, filter, flatMap --> Narrow Transformation

# reduceByKey, groupByKey --> Wide Transformation

# Broadcast the dataset which lower in size, so it will be available on all the nodes for join and won't occupy more space
# spark.sparkContext.broadcast(customers.collect())

# joined_rdd = orders.join(customers)

# Repartition Vs Coalesce

# Repartitioning is the process of changing the number of partitions of an RDD either increasing or decreasing.
# Coalescing is the process of changing the number of partitions of an RDD only for decreasing.
# Repartitioning or Coalesce can be done by specifying the number of partitions.
#
# Coalesce is more performant compared to repartition because it tries to merge partitions on same node to form a new partition that could be of unequal size but shuffling is avoided.
# Repartition do complete shuffling with intent to have equal partition size.

repartitioned_rdd = order_rdd.repartition(6)
coalesced_rdd = order_rdd.coalesce(1)

