from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("accessing-columns").enableHiveSupport().getOrCreate()

log_data_path = get_config['logdata1m']
schema = "loglevel string, logtime string"
log_df = spark.read.schema(schema).csv(log_data_path)
log_df.show()

new_log_df = log_df.withColumn("logtime", to_timestamp("logtime"))
new_log_df.createOrReplaceTempView("log_view")

spark.sql("select * from log_view").show()
spark.sql("select loglevel, date_format(logtime, 'MM') as month from log_view").show()
spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month, date_format(logtime, 'MM') as month_no, count(*) as total_occurence from log_view group by loglevel, month, month_no order by month_no""").show()
result_df = spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month, first(date_format(logtime, 'MM')) as month_no, count(*) as total_occurence from log_view group by loglevel, month order by month_no""")
final_df = result_df.drop("month_no")
final_df.show()

# Pivot table optimization
month_list = ["January","February", "March","April", "May", "June", "July", "August", "September", "October", "November", "December"]
spark.sql("""select loglevel, date_format(logtime, 'MMMM') as month from log_view""").groupBy("loglevel").pivot("month", month_list).count().show()