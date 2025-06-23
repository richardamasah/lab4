import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, unix_timestamp, col, count, sum, max, min, avg

# Input arguments
transactions_path = sys.argv[1]
users_path = sys.argv[2]
output_path = sys.argv[3]

# Start Spark
spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()

# Load data
transactions_df = spark.read.option("header", True).option("inferSchema", True).csv(transactions_path)
users_df = spark.read.option("header", True).option("inferSchema", True).csv(users_path)

# Convert to timestamp
transactions_df = transactions_df.withColumn("rental_start_time", to_timestamp("rental_start_time")) \
                                 .withColumn("rental_end_time", to_timestamp("rental_end_time"))

# Add rental duration
transactions_df = transactions_df.withColumn("rental_duration_hours",
    (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600)

# Add rental_date
transactions_df = transactions_df.withColumn("rental_date", to_date("rental_start_time"))

# ---------- Metrics 1: Daily Metrics ---------
daily_metrics = transactions_df.groupBy("rental_date").agg(
    count("*").alias("total_transactions"),
    sum("total_amount").alias("total_revenue")
)

# ---------- Metrics 2: User Metrics ----------
user_metrics = transactions_df.groupBy("user_id").agg(
    count("*").alias("user_total_transactions"),
    sum("total_amount").alias("user_total_spent"),
    sum("rental_duration_hours").alias("user_total_hours"),
    max("total_amount").alias("user_max_transaction"),
    min("total_amount").alias("user_min_transaction"),
    avg("total_amount").alias("user_avg_transaction")
)

# Join with user info
user_metrics = user_metrics.join(users_df, on="user_id", how="left")

# ---------- Write Output ----------
daily_metrics.write.mode("overwrite").parquet(f"{output_path}daily_metrics")
user_metrics.write.mode("overwrite").parquet(f"{output_path}user_metrics")

print("✅ Job 2 Completed Successfully..")
