from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from config.config import Config
config = Config()

get_file = config.get_config()
groceries_file = get_file['groceries']

spark = SparkSession.builder.master("local[*]").appName("week5-question1").enableHiveSupport().getOrCreate()

groceries_df = spark.read.csv(groceries_file, inferSchema=True, header=True)
groceries_df.createOrReplaceTempView("groceries")

spark.sql("create database if not exists haresh_demo")
spark.sql("show databases").show()
spark.sql("use haresh_demo")
spark.sql('create table if not exists haresh_demo.groceries_new (order_id string, location string, item string, order_date string, quantity integer)')
spark.sql("insert into table haresh_demo.groceries_new select * from groceries")
spark.sql("select count(*) from groceries_new").show()

spark.sql("create table if not exists haresh_demo.groceries_ext (order_id string, location string, item string, order_date string, quantity integer) using csv options(header='true') location '/home/haresh/work/files/groceries/groceries.csv'")
spark.sql("select * from haresh_demo.groceries_ext limit 10").show()
spark.sql("select * from haresh_demo.groceries_new limit 10").show()
spark.sql("describe extended haresh_demo.groceries_ext").show(truncate=False)

spark.sql("drop table haresh_demo.groceries_ext")
spark.sql("drop table haresh_demo.groceries_new")

spark.sql("show tables").show()




