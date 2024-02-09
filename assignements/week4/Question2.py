from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from config.config import Config

config = Config()
get_config = config.get_config()

spark = SparkSession.builder.master("local[*]").appName("week4-question1").enableHiveSupport().getOrCreate()

cases = get_config['covid19_cases']
states = get_config['covid19_states']

cases_rdd = spark.sparkContext.textFile(cases)
states_rdd = spark.sparkContext.textFile(states)

# 1. Find the top 10 states with the highest no.of positive cases.
state_positive_map = cases_rdd.map(lambda x : (x.split(",")[1], int(x.split(",")[2])))
highest_positive_states = state_positive_map.reduceByKey(lambda x, y: x + y).sortBy(lambda x : x[1], ascending=False)

for item in highest_positive_states.take(10):
    print(item)

# 2 . Find the total count of people in ICU currently
icu_map = cases_rdd.map(lambda x : int(x.split(",")[7])).sum()
print("Total in ICU : --> " + str(icu_map))

# 3. Top 15 states having max number of recovery
state_recovery_map = cases_rdd.map(lambda x : (x.split(",")[1], int(x.split(",")[11])))
highest_recovery_states = state_recovery_map.reduceByKey(lambda x, y: x + y).sortBy(lambda x : x[1], ascending=False)

for item in highest_recovery_states.take(10):
    print(item)

# 4. Find the top 3 states having the least no of deaths.
state_death_map = cases_rdd.map(lambda x : (x.split(",")[1], int(x.split(",")[23])))
least_death_states = state_death_map.reduceByKey(lambda x, y: x + y).sortBy(lambda x : x[1])
print("-------- least deaths -----------")
for item in least_death_states.take(10):
    print(item)

# 5.Find the total number of people hospitalized currently
hospitalized_map = cases_rdd.map(lambda x : int(x.split(",")[5])).sum()
print("Total in hospitalized : --> " + str(hospitalized_map))

# 6.List the twitter handle and fips code for the top 15 states with the highest number of total cases.
state_twitter_fips = states_rdd.map(lambda x : (x.split(",")[0], (x.split(",")[5], int(x.split(",")[8]))))
for case in state_twitter_fips.take(10):
    print(case)

state_total_cases_per_day = cases_rdd.map(lambda x : (x.split(",")[1], int(x.split(",")[28])))
state_total_cases = state_total_cases_per_day.reduceByKey(lambda x, y : x + y)
for case in state_total_cases.take(10):
    print(case)

print("------(state, (twitter, fips, total))-------")
joined_map = state_twitter_fips.join(state_total_cases)
for case in joined_map.take(10):
    print(case)

print("------(state, witter, fips, total))-------")
mapped_join = joined_map.sortBy(lambda x : x[1][1], ascending=False)
for case in mapped_join.take(10):
    print(case)
