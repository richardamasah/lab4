from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, count, avg, max, min, to_timestamp, unix_timestamp

# Start Spark session
spark = SparkSession.builder.appName("VehicleLocationPerformance").getOrCreate()

# Input paths
transactions_path = "s3://lab4emr1/raw-data/rental_transactions/"
vehicles_path = "s3://lab4emr1/raw-data/vehicles/"
locations_path = "s3://lab4emr1/raw-data/locations/"

# Output path
output_path = "s3://lab4emr1/processed/job1_output/"

# Load datasets
transactions_df = spark.read.option("header", True).csv(transactions_path)
vehicles_df = spark.read.option("header", True).csv(vehicles_path)
locations_df = spark.read.option("header", True).csv(locations_path)

# Convert time columns to timestamp
transactions_df = transactions_df.withColumn("rental_start_time", to_timestamp("rental_start_time")) \
                                 .withColumn("rental_end_time", to_timestamp("rental_end_time"))

# Compute rental duration in hours
transactions_df = transactions_df.withColumn("rental_duration_hours",
    (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600)

# JOIN 1: transactions + vehicles
tx_vehicles_df = transactions_df.join(vehicles_df, on="vehicle_id", how="left")

# JOIN 2: transactions + locations (pickup)
tx_loc_df = transactions_df.join(locations_df, transactions_df.pickup_location == locations_df.location_id, "left")

# ---------- METRICS 1: BY LOCATION ----------
location_metrics = tx_loc_df.groupBy("pickup_location", "location_name").agg(
    sum("total_amount").alias("total_revenue"),
    count("*").alias("total_transactions"),
    avg("total_amount").alias("avg_transaction"),
    max("total_amount").alias("max_transaction"),
    min("total_amount").alias("min_transaction"),
    countDistinct("vehicle_id").alias("unique_vehicles")
)

# ---------- METRICS 2: BY VEHICLE TYPE ----------
vehicle_metrics = tx_vehicles_df.groupBy("vehicle_type").agg(
    sum("rental_duration_hours").alias("total_rental_hours"),
    sum("total_amount").alias("total_revenue")
)

# ---------- SAVE TO PARQUET ----------
location_metrics.write.mode("overwrite").parquet(f"{output_path}location_metrics")
vehicle_metrics.write.mode("overwrite").parquet(f"{output_path}vehicle_metrics")

print("✅ Job 1 Completed Successfully")
