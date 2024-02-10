from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, IntegerType, DoubleType

from config.config import Config
config = Config()

get_file = config.get_config()
customer_files = get_file['customers']

spark = SparkSession.builder.master("local[*]").appName("week5-question3").enableHiveSupport().getOrCreate()

schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("cust_fname", StringType(), True),
    StructField("cust_lname", StringType(), True),
    StructField("cust_email", StringType(), True),
    StructField("cust_password", StringType(), True),
    StructField("cust_street", StringType(), True),
    StructField("cust_city", StringType(), True),
    StructField("cust_state", StringType(), True),
    StructField("cust_zipcode", IntegerType(), True)
])

customer_df = spark.read.csv(customer_files, schema=schema)
customer_df.show()
customer_df.createOrReplaceTempView("customer_temp")

# 3.1 Find the total number of customers in each state
customer_df.groupBy("cust_state").count().orderBy("count", ascending=False).show(5)
spark.sql("select cust_state, count(*) cnt from customer_temp group by cust_state order by cnt desc").show(5)

# 3.2 FInd the top 5 most common last names among the customers
customer_df.groupBy("cust_lname").count().orderBy("count", ascending=False).show(5)
spark.sql("select cust_lname, count(*) cnt from customer_temp group by cust_lname order by cnt desc").show(5)

# 3.3. Check whether there are any customers whose zip codes are not valid(i.e not equal to 5 digits)
invalid_zips = customer_df.filter(length("cust_zipcode") != 5)
if invalid_zips.count() == 0:
    print("All customers have valid zip code")
else:
    print("There are customers with invalid zip codes")
    invalid_zips.show()
spark.sql("select * from customer_temp where length(cust_zipcode) != 5").show()

# 3.4 Count the number of customers who have valid zip codes.
valid_zips = customer_df.filter(length("cust_zipcode") == 5).count()
print("Customers with valid zip code: " + str(valid_zips))

# 3.5. Find the number of customers from each city in the state of California(CA).
customer_df.filter("cust_state = 'CA'").groupBy("cust_city").count().show()
spark.sql("select cust_city, count(*) from customer_temp where cust_state = 'CA' group by cust_city").show()


