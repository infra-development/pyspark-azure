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

words = ("big", "Data", "Is", "SUPER", "Interesting", "BIG", "data", "IS", "A", "Trending", "technology")

words_rdd = spark.sparkContext.parallelize(words)

words_normalized = words_rdd.map(lambda x: (x.lower(), 1))

reduced_rdd = words_normalized.reduceByKey(lambda x, y: x + y)

reduced_rdd.collect()

