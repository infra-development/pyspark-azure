from pyspark.sql import SparkSession
from config import Config

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

#spark.sparkContext.broadcast(customers.collect())

# Repartition Vs Coalesce

repartitioned_rdd = order_rdd.repartition(6)
coalesced_rdd = order_rdd.coalesce(1)

