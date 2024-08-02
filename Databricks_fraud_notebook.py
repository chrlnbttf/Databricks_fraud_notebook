# Databricks notebook source
## Import of libraries and modules
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import col, countDistinct, when, lit, udf, from_unixtime, to_timestamp, min, max, avg, count, unix_timestamp
import random
import json
import time

# COMMAND ----------

## Data Ingestion : Regarding Transactions (CSV=> DELTA) and client profiles (PARQUET=>DELTA)

# Create Spark session
spark = SparkSession.builder.appName("FinancialFraudDetection").getOrCreate()

# Load Transactions Bancaires (CSV)
transactions_df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/fraud_detection_sample", header=True, inferSchema=True)

# Load Données de Profil Client (Parquet)
profiles_df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/customer_profiles")


# COMMAND ----------

## Data ingestion : Creation of Log frequency dataset and Ingestion of Logs (DELTA)


## Step 1 : Timeframe of the Transactions dataset

# Conversion of the Unix timestamp into a human-readable string
transactions_df = transactions_df.withColumn("transaction_time", from_unixtime(col("unix_time")))

# Timeframe of transactions_df
max_transaction_time = transactions_df.agg(max("transaction_time")).collect()[0][0]
min_transaction_time = transactions_df.agg(min("transaction_time")).collect()[0][0]
print(max_transaction_time)
print(min_transaction_time)

# Time difference of transactions_df after conversion into timestamp = datetime
date1 = datetime.strptime(min_transaction_time, "%Y-%m-%d %H:%M:%S") 
date2 = datetime.strptime(max_transaction_time, "%Y-%m-%d %H:%M:%S")
diff = date2 - date1
print(diff)

result = (date2 - timedelta(days=random.randint(0, diff.days))).isoformat()
print(result)

## Step 2 : Creation of Log frequency dataset

def generate_login_logs(num_logs=10000):
    logs = []
    for _ in range(num_logs):
        log = {
            "account_id": random.randint(1, 10000),
            "session_id": random.randint(100000, 999999),
            "login_time": (date2 - timedelta(days=random.randint(0, diff.days))).isoformat(),
            "ip_address": ".".join(map(str, (random.randint(1, 255) for _ in range(4))))
        }
        logs.append(log)

    # Write logs to Delta table
    spark.createDataFrame(logs).write.format("delta").mode("overwrite").saveAsTable("login_logs")

generate_login_logs()

## Step 3 : Load

logs_df = spark.read.table("login_logs")
display(logs_df)

# COMMAND ----------

## Dataframe Insight menu

# transactions_df
#transactions_df.printSchema()
#transactions_df.select("transaction_time").show(5)
#transactions_df.select("Unnamed: 0").show(5)
#transactions_df.show(5)

# logs_df
#logs_df.printSchema()
#logs_df.select("login_time").show(5)
#logs_df.show(5)
# NB : Conversion of the string column "login_time" into timestamp : logs_df = logs_df.withColumn("log_time", to_timestamp(col("login_time")))

# profiles_df
#profiles_df.printSchema()
#profiles_df.show(5)

# Duplicates ?
#print(transactions_df.distinct().count())
#print(logs_df.distinct().count())
#print(profiles_df.distinct().count())
#print(transactions_df.count())
#print(logs_df.count())
#print(profiles_df.count())


# COMMAND ----------

## Data Transformation : Injection of credit card numbers randomly and inheritated from transactions_df into profiles_client to create a common key


# Select the distinct values of 'cc_num' from transactions_df
distinct_cc_nums = transactions_df.select("cc_num").distinct().rdd.map(lambda row: row[0]).collect()
num_distinct_cc_nums = len(distinct_cc_nums)
print(f"Nombre de valeurs distinctes dans transactions_df['cc_num'] : {num_distinct_cc_nums}")

# Partial inheritance from transactions_df values : Data injection using pandas to avoid creating an index with pyspark
profiles_df = profiles_df.withColumn("cc_num", lit(None))
profiles_pd = profiles_df.toPandas()
profiles_pd.loc[:910, "cc_num"] = distinct_cc_nums

import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items # to avoid the error "'DataFrame' object has no attribute 'iteritems'""

profiles_df = spark.createDataFrame(profiles_pd)

# Partial population with random card numbers
def generate_random_cc_number():
    length = random.choice([15, 16, 17])
    return int("".join([str(random.randint(0, 9)) for _ in range(length)]))

generate_random_cc_number_udf = udf(generate_random_cc_number)
profiles_df = profiles_df.withColumn("cc_num", when(col("cc_num").isNull(), generate_random_cc_number_udf()).otherwise(col("cc_num")))

# Display of the results
profiles_df.show()
print("The number of lines in profiles_df is : ", profiles_df.count())
profiles_df.agg(countDistinct(col('CUST_ID')).alias('distinct_cust_id')).show()
profiles_df.agg(countDistinct(col('cc_num')).alias('distinct_cc_num')).show()


# COMMAND ----------

## Data Transformation : Temporal information-based matching to create a common key between transactions_df and logs_df

# Test of time formates 
example_tr = transactions_df.select("transaction_time").collect()[0][0]
example_tr=str(example_tr)
example_tr_dt = datetime.strptime(example_tr, "%Y-%m-%d %H:%M:%S") #Standard Datetime Format matching the string
print(example_tr)
print(example_tr_dt)

example_lo = logs_df.select("login_time").collect()[0][0]
example_lo=str(example_lo)
example_lo_dt = datetime.strptime(example_lo, "%Y-%m-%dT%H:%M:%S") # SO 8601 Format matching the string
print(example_lo)
print(example_lo_dt)

print(example_tr_dt - example_lo_dt)

# Convert login_time from string to timestamp
logs_df = logs_df.withColumn("log_time", col("login_time").cast("timestamp"))



# COMMAND ----------

## Data integration

# Join of DataFrames transactions_df and logs_df

#Use of a 60minutes log connection window
joined_df = transactions_df.join(
    logs_df,
    (unix_timestamp(col("transaction_time")) - unix_timestamp(col("log_time"))).between(-1800, 1800),
    "inner"
)
joined_df.show(5)
#joined_df.count() # 8411 rows

#Note : Count of matched transactions_df and profiles_df rows
transactions_df.select("cc_num").show(5)
profiles_df.select("cc_num").show(5)
matched_df = transactions_df.join(profiles_df, transactions_df.cc_num == profiles_df.cc_num, "inner")
matched_df.show(5)
matched_df.count() # 12967 rows

#Join of all dataframes, count of rows and time execution measure
start = time.time()
combined_df = joined_df.join(profiles_df, transactions_df.cc_num == profiles_df.cc_num, "inner")
combined_df.show(5)
combined_df.count()
end = time.time()
exec_time = end - start
print(exec_time) # 2min locally versus 2min16s on Databricks

# COMMAND ----------

## Creation of flags as potential fraud indicator (amount per user, logs frequency, unusual locations etc)

import pyspark.sql.functions as F

# high amount per user flag
#Find the transaction amount per user
transactions_avg_df = transactions_df.groupBy("cc_num").agg(avg("amt").alias("avg_transaction_amount"))
transactions_df_with_avg = transactions_df.join(transactions_avg_df, "cc_num", "left")
#Create the flag : transaction amount above 1000
transactions_df_high = transactions_df_with_avg.withColumn("high_transaction", col("avg_transaction_amount") > 1000)
transactions_df_high_count = transactions_df_high.filter(F.col("high_transaction")).count()
#Compare with the number of transaction
print(f"There is {transactions_df_high_count} transactions up to 1000€ out of a total of {transactions_df.count()} transactions")

#high log frequency flag
#Find the frequency of logs
login_frequency_df = logs_df.groupBy("account_id").agg(count("session_id").alias("login_frequency"))
logs_df_with_frequency = logs_df.join(login_frequency_df, "account_id", "left")
#Create the flag : login frequency above 5
logs_df_high = logs_df_with_frequency.withColumn("high_connection", col("login_frequency") > 5)
logs_df_high_count = logs_df_high.filter(F.col("high_connection")).count()
#Compare with the number of distinct logs
logs_df_distinct_logs = logs_df_with_frequency.agg(countDistinct(col('account_id')).alias('distinct_account_id'))
logs_df_distinct_logs_value = logs_df_distinct_logs.select("distinct_account_id").collect()[0][0]
print(f"There is {logs_df_high_count} connections above 5 connections per user concerning 6 users compared out of a total of {logs_df_distinct_logs_value} connections")


