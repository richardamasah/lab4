import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, count, avg, max, min, to_timestamp, unix_timestamp

# ---------------------------------------
# Read input arguments from command line
# ---------------------------------------
transactions_path = sys.argv[1]
vehicles_path = sys.argv[2]
locations_path = sys.argv[3]
output_path = sys.argv[4]

# ---------------------------------------
# Start Spark session
# ---------------------------------------
spark = SparkSession.builder.appName("VehicleLocationPerformance").getOrCreate()

# ---------------------------------------
# Load datasets from S3
# ---------------------------------------


transactions_df = spark.read.option("header", True).option("inferSchema", True).csv(transactions_path)
vehicles_df = spark.read.option("header", True).option("inferSchema", True).csv(vehicles_path)
locations_df = spark.read.option("header", True).option("inferSchema", True).csv(locations_path)


# ---------------------------------------
# Data Preprocessing
# ---------------------------------------
# Convert to timestamp
transactions_df = transactions_df.withColumn("rental_start_time", to_timestamp("rental_start_time")) \
                                 .withColumn("rental_end_time", to_timestamp("rental_end_time"))

# Calculate rental duration in hours
transactions_df = transactions_df.withColumn("rental_duration_hours",
    (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600)

# ---------------------------------------
# Join with vehicles and locations
# ---------------------------------------
tx_vehicles_df = transactions_df.join(vehicles_df, on="vehicle_id", how="left")
tx_loc_df = transactions_df.join(locations_df, transactions_df.pickup_location == locations_df.location_id, "left")

# ---------------------------------------
# Metrics 1: By Location
# ---------------------------------------
location_metrics = tx_loc_df.groupBy("pickup_location", "location_name").agg(
    sum("total_amount").alias("total_revenue"),
    count("*").alias("total_transactions"),
    avg("total_amount").alias("avg_transaction"),
    max("total_amount").alias("max_transaction"),
    min("total_amount").alias("min_transaction"),
    countDistinct("vehicle_id").alias("unique_vehicles")
)

# ---------------------------------------
# Metrics 2: By Vehicle Type
# ---------------------------------------
vehicle_metrics = tx_vehicles_df.groupBy("vehicle_type").agg(
    sum("rental_duration_hours").alias("total_rental_hours"),
    sum("total_amount").alias("total_revenue")
)

# ---------------------------------------
# Save Outputs to Parquet in S3
# ---------------------------------------
location_metrics.write.mode("overwrite").parquet(f"{output_path}location_metrics")
vehicle_metrics.write.mode("overwrite").parquet(f"{output_path}vehicle_metrics")

print("✅ Job 1 Completed Successfully.")
