import sys
import logging # Import the logging module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, count, avg, max, min, to_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.utils import AnalysisException

# ---------------------------------------
# Configure Logging
# ---------------------------------------
# Get the root logger
logger = logging.getLogger(__name__)
# Set the logging level (e.g., INFO, DEBUG, WARNING, ERROR, CRITICAL)
logger.setLevel(logging.INFO)

# Create a console handler and set its level
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Create a formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handler to the logger
# Ensure handlers are not duplicated if the script is run multiple times in the same process
if not logger.handlers:
    logger.addHandler(console_handler)

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
        logger.error("Incorrect number of arguments provided.")
        logger.info("Usage: spark-submit your_script.py <transactions_s3_path> <vehicles_s3_path> <locations_s3_path> <output_s3_path>")
        sys.exit(1) # Exit with an error code

    transactions_path = sys.argv[1]
    vehicles_path = sys.argv[2]
    locations_path = sys.argv[3]
    output_path = sys.argv[4]

    logger.info(f"Starting Spark Job: VehicleLocationPerformance")
    logger.info(f"Input Transactions Path: {transactions_path}")
    logger.info(f"Input Vehicles Path: {vehicles_path}")
    logger.info(f"Input Locations Path: {locations_path}")
    logger.info(f"Output Path: {output_path}")

    # ---------------------------------------
    # 2. Start Spark session
    # ---------------------------------------
    try:
        spark = SparkSession.builder.appName("VehicleLocationPerformance").getOrCreate()
        logger.info("Spark session created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}", exc_info=True) # exc_info=True adds traceback
        sys.exit(1) # Exit if Spark session cannot be created

    # ---------------------------------------
    # 3. Define Schemas for Input DataFrames
    # ---------------------------------------
    # Explicitly defining schemas ensures data types are correctly interpreted,
    # preventing issues that can arise from inferSchema=True.

    # Schema for locations dataset
    locations_schema = StructType([
        StructField("location_id", IntegerType(), True),
        StructField("location_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", IntegerType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])
    logger.info("Defined schema for locations data.")

    # Schema for rental_transactions dataset
    # Note: rental_start_time and rental_end_time are read as StringType initially,
    # then converted to TimestampType in the preprocessing step.
    transactions_schema = StructType([
        StructField("rental_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("rental_start_time", StringType(), True), # Will be converted to TimestampType
        StructField("rental_end_time", StringType(), True),   # Will be converted to TimestampType
        StructField("pickup_location", IntegerType(), True),
        StructField("dropoff_location", IntegerType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    logger.info("Defined schema for rental_transactions data.")

    # Schema for vehicles dataset
    vehicles_schema = StructType([
        StructField("active", IntegerType(), True),
        StructField("vehicle_license_number", StringType(), True),
        StructField("registration_name", StringType(), True),
        StructField("license_type", StringType(), True),
        StructField("expiration_date", StringType(), True),
        StructField("permit_license_number", StringType(), True),
        StructField("certification_date", StringType(), True),
        StructField("vehicle_year", IntegerType(), True),
        StructField("base_telephone_number", StringType(), True),
        StructField("base_address", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("last_update_timestamp", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("vehicle_type", StringType(), True)
    ])
    logger.info("Defined schema for vehicles data.")

    # ---------------------------------------
    # 4. Load datasets from S3 using defined schemas
    # ---------------------------------------
    # Using try-except blocks to catch potential issues during data loading,
    # such as incorrect paths or corrupted files.
    try:
        # Load transactions data with its defined schema
        transactions_df = spark.read.option("header", True).schema(transactions_schema).csv(transactions_path)
        logger.info(f"Loaded transactions data from: {transactions_path}. Rows: {transactions_df.count()}")

        # Load vehicles data with its defined schema
        vehicles_df = spark.read.option("header", True).schema(vehicles_schema).csv(vehicles_path)
        logger.info(f"Loaded vehicles data from: {vehicles_path}. Rows: {vehicles_df.count()}")

        # Load locations data with its defined schema
        locations_df = spark.read.option("header", True).schema(locations_schema).csv(locations_path)
        logger.info(f"Loaded locations data from: {locations_path}. Rows: {locations_df.count()}")

    except AnalysisException as e:
        logger.error(f"Failed to load one or more datasets. Check S3 paths, file formats, and schema definitions. Error: {e}", exc_info=True)
        spark.stop()
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred during data loading: {e}", exc_info=True)
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 5. Data Preprocessing
    # ---------------------------------------
    # Convert 'rental_start_time' and 'rental_end_time' to timestamp type
    # This is crucial for date/time calculations.
    # Calculate 'rental_duration_hours' from start and end timestamps.
    try:
        transactions_df = transactions_df.withColumn("rental_start_time", to_timestamp("rental_start_time")) \
                                         .withColumn("rental_end_time", to_timestamp("rental_end_time"))
        transactions_df = transactions_df.withColumn("rental_duration_hours",
                                         (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600)
        logger.info("Successfully preprocessed rental transactions data (timestamp conversion, duration calculation).")
        # You might want to add data validation here, e.g., filter out records where rental_duration_hours is negative
        # transactions_df = transactions_df.filter(col("rental_duration_hours") >= 0)
    except Exception as e:
        logger.error(f"Failed during data preprocessing (timestamp conversion or duration calculation): {e}", exc_info=True)
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 6. Join with vehicles and locations
    # ---------------------------------------
    # Join transactions with vehicles data on 'vehicle_id' to enrich transaction data.
    # Join transactions with locations data on 'pickup_location' and 'location_id' to get location details.
    try:
        tx_vehicles_df = transactions_df.join(vehicles_df, on="vehicle_id", how="left")
        logger.info("Joined transactions with vehicles data.")

        tx_loc_df = transactions_df.join(locations_df, transactions_df.pickup_location == locations_df.location_id, "left")
        logger.info("Joined transactions with locations data.")
    except Exception as e:
        logger.error(f"Failed during DataFrame joins: {e}", exc_info=True)
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 7. Metrics 1: By Location Performance
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
        logger.info("Calculated location performance metrics.")
        # Show a sample of the results for verification - this will still print to stdout
        location_metrics.show(5)
    except Exception as e:
        logger.error(f"Failed during calculation of location metrics: {e}", exc_info=True)
        spark.stop()
        sys.exit(1)


    # ---------------------------------------
    # 8. Metrics 2: By Vehicle Type Performance
    # ---------------------------------------
    # Group by vehicle type to calculate aggregate metrics:
    # - total_rental_hours: Sum of 'rental_duration_hours'
    # - total_revenue: Sum of 'total_amount' for each vehicle type
    try:
        vehicle_metrics = tx_vehicles_df.groupBy("vehicle_type").agg(
            sum("rental_duration_hours").alias("total_rental_hours"),
            sum("total_amount").alias("total_revenue")
        )
        logger.info("Calculated vehicle type performance metrics.")
        # Show a sample of the results for verification - this will still print to stdout
        vehicle_metrics.show(5)
    except Exception as e:
        logger.error(f"Failed during calculation of vehicle type metrics: {e}", exc_info=True)
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 9. Save Outputs to Parquet in S3
    # ---------------------------------------
    # Write the calculated metrics to S3 in Parquet format.
    # 'overwrite' mode will replace the output directory if it already exists.
    try:
        # Output for location metrics
        location_metrics_output_path = f"{output_path}location_metrics"
        location_metrics.write.mode("overwrite").parquet(location_metrics_output_path)
        logger.info(f"Successfully saved location metrics to: {location_metrics_output_path}")

        # Output for vehicle metrics
        vehicle_metrics_output_path = f"{output_path}vehicle_metrics"
        vehicle_metrics.write.mode("overwrite").parquet(vehicle_metrics_output_path)
        logger.info(f"Successfully saved vehicle metrics to: {vehicle_metrics_output_path}")

        logger.info(" Job 1 Completed Successfully.")
    except Exception as e:
        logger.error(f"Failed to save outputs to S3: {e}", exc_info=True)
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 10. Stop Spark session
    # ---------------------------------------
    spark.stop()
    logger.info("Spark session stopped.")

