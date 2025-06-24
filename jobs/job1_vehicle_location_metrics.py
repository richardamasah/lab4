import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, count, avg, max, min, to_timestamp, unix_timestamp
from pyspark.sql.utils import AnalysisException # Import for handling potential schema inference/read errors

# Main execution block for the PySpark script
if __name__ == "__main__":
    # ---------------------------------------
    # 1. Read input arguments from command line
    # ---------------------------------------
    # This script expects four arguments:
    # 1. Path to the rental_transactions dataset in S3
    # 2. Path to the vehicles dataset in S3
    # 3. Path to the locations dataset in S3
    # 4. Output path in S3 for processed data
    if len(sys.argv) != 5:
        print("Error: Incorrect number of arguments provided.")
        print("Usage: spark-submit your_script.py <transactions_s3_path> <vehicles_s3_path> <locations_s3_path> <output_s3_path>")
        sys.exit(1) # Exit with an error code

    transactions_path = sys.argv[1]
    vehicles_path = sys.argv[2]
    locations_path = sys.argv[3]
    output_path = sys.argv[4]

    print(f"INFO: Starting Spark Job: VehicleLocationPerformance")
    print(f"INFO: Input Transactions Path: {transactions_path}")
    print(f"INFO: Input Vehicles Path: {vehicles_path}")
    print(f"INFO: Input Locations Path: {locations_path}")
    print(f"INFO: Output Path: {output_path}")

    # ---------------------------------------
    # 2. Start Spark session
    # ---------------------------------------
    try:
        spark = SparkSession.builder.appName("VehicleLocationPerformance").getOrCreate()
        print("INFO: Spark session created successfully.")
    except Exception as e:
        print(f"ERROR: Failed to create Spark session: {e}")
        sys.exit(1) # Exit if Spark session cannot be created

    # ---------------------------------------
    # 3. Load datasets from S3
    # ---------------------------------------
    # Using try-except blocks to catch potential issues during data loading,
    # such as incorrect paths or corrupted files.
    try:
        transactions_df = spark.read.option("header", True).option("inferSchema", True).csv(transactions_path)
        print(f"INFO: Loaded transactions data from: {transactions_path}. Rows: {transactions_df.count()}")

        vehicles_df = spark.read.option("header", True).option("inferSchema", True).csv(vehicles_path)
        print(f"INFO: Loaded vehicles data from: {vehicles_path}. Rows: {vehicles_df.count()}")

        locations_df = spark.read.option("header", True).option("inferSchema", True).csv(locations_path)
        print(f"INFO: Loaded locations data from: {locations_path}. Rows: {locations_df.count()}")

    except AnalysisException as e:
        print(f"ERROR: Failed to load one or more datasets. Check S3 paths and file formats. Error: {e}")
        spark.stop()
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during data loading: {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 4. Data Preprocessing
    # ---------------------------------------
    # Convert 'rental_start_time' and 'rental_end_time' to timestamp type
    # This is crucial for date/time calculations.
    # Calculate 'rental_duration_hours' from start and end timestamps.
    try:
        transactions_df = transactions_df.withColumn("rental_start_time", to_timestamp("rental_start_time")) \
                                         .withColumn("rental_end_time", to_timestamp("rental_end_time"))
        transactions_df = transactions_df.withColumn("rental_duration_hours",
                                         (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600)
        print("INFO: Successfully preprocessed rental transactions data (timestamp conversion, duration calculation).")
        # You might want to add data validation here, e.g., filter out records where rental_duration_hours is negative
        # transactions_df = transactions_df.filter(col("rental_duration_hours") >= 0)
    except Exception as e:
        print(f"ERROR: Failed during data preprocessing (timestamp conversion or duration calculation): {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 5. Join with vehicles and locations
    # ---------------------------------------
    # Join transactions with vehicles data on 'vehicle_id' to enrich transaction data.
    # Join transactions with locations data on 'pickup_location' and 'location_id' to get location details.
    try:
        tx_vehicles_df = transactions_df.join(vehicles_df, on="vehicle_id", how="left")
        print("INFO: Joined transactions with vehicles data.")

        tx_loc_df = transactions_df.join(locations_df, transactions_df.pickup_location == locations_df.location_id, "left")
        print("INFO: Joined transactions with locations data.")
    except Exception as e:
        print(f"ERROR: Failed during DataFrame joins: {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 6. Metrics 1: By Location Performance
    # ---------------------------------------
    # Group by pickup location and location name to calculate aggregate metrics:
    # - total_revenue: Sum of 'total_amount'
    # - total_transactions: Count of all transactions
    # - avg_transaction: Average of 'total_amount'
    # - max_transaction: Maximum 'total_amount'
    # - min_transaction: Minimum 'total_amount'
    # - unique_vehicles: Count of distinct vehicle IDs used at each location
    try:
        location_metrics = tx_loc_df.groupBy("pickup_location", "location_name").agg(
            sum("total_amount").alias("total_revenue"),
            count("*").alias("total_transactions"),
            avg("total_amount").alias("avg_transaction"),
            max("total_amount").alias("max_transaction"),
            min("total_amount").alias("min_transaction"),
            countDistinct("vehicle_id").alias("unique_vehicles")
        )
        print("INFO: Calculated location performance metrics.")
        # Show a sample of the results for verification
        location_metrics.show(5)
    except Exception as e:
        print(f"ERROR: Failed during calculation of location metrics: {e}")
        spark.stop()
        sys.exit(1)


    # ---------------------------------------
    # 7. Metrics 2: By Vehicle Type Performance
    # ---------------------------------------
    # Group by vehicle type to calculate aggregate metrics:
    # - total_rental_hours: Sum of 'rental_duration_hours'
    # - total_revenue: Sum of 'total_amount' for each vehicle type
    try:
        vehicle_metrics = tx_vehicles_df.groupBy("vehicle_type").agg(
            sum("rental_duration_hours").alias("total_rental_hours"),
            sum("total_amount").alias("total_revenue")
        )
        print("INFO: Calculated vehicle type performance metrics.")
        # Show a sample of the results for verification
        vehicle_metrics.show(5)
    except Exception as e:
        print(f"ERROR: Failed during calculation of vehicle type metrics: {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 8. Save Outputs to Parquet in S3
    # ---------------------------------------
    # Write the calculated metrics to S3 in Parquet format.
    # 'overwrite' mode will replace the output directory if it already exists.
    try:
        # Output for location metrics
        location_metrics_output_path = f"{output_path}location_metrics"
        location_metrics.write.mode("overwrite").parquet(location_metrics_output_path)
        print(f"INFO: Successfully saved location metrics to: {location_metrics_output_path}")

        # Output for vehicle metrics
        vehicle_metrics_output_path = f"{output_path}vehicle_metrics"
        vehicle_metrics.write.mode("overwrite").parquet(vehicle_metrics_output_path)
        print(f"INFO: Successfully saved vehicle metrics to: {vehicle_metrics_output_path}")

        print("✅ Job 1 Completed Successfully.")
    except Exception as e:
        print(f"ERROR: Failed to save outputs to S3: {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 9. Stop Spark session
    # ---------------------------------------
    spark.stop()
    print("INFO: Spark session stopped.")