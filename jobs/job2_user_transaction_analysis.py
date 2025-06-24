import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, unix_timestamp, col, count, sum, max, min, avg
from pyspark.sql.utils import AnalysisException # Import for handling potential schema inference/read errors

# Main execution block for the PySpark script
if __name__ == "__main__":
    # ---------------------------------------
    # 1. Read input arguments from command line
    # ---------------------------------------
    # This script expects three arguments:
    # 1. Path to the rental_transactions dataset in S3
    # 2. Path to the users dataset in S3
    # 3. Output path in S3 for processed data
    if len(sys.argv) != 4:
        print("Error: Incorrect number of arguments provided.")
        print("Usage: spark-submit your_script.py <transactions_s3_path> <users_s3_path> <output_s3_path>")
        sys.exit(1) # Exit with an error code

    transactions_path = sys.argv[1]
    users_path = sys.argv[2]
    output_path = sys.argv[3]

    print(f"INFO: Starting Spark Job: UserTransactionAnalysis")
    print(f"INFO: Input Transactions Path: {transactions_path}")
    print(f"INFO: Input Users Path: {users_path}")
    print(f"INFO: Output Path: {output_path}")

    # ---------------------------------------
    # 2. Start Spark session
    # ---------------------------------------
    try:
        spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()
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

        users_df = spark.read.option("header", True).option("inferSchema", True).csv(users_path)
        print(f"INFO: Loaded users data from: {users_path}. Rows: {users_df.count()}")

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
    # Add 'rental_duration_hours' by calculating the difference between start and end times.
    # Add 'rental_date' by extracting the date part from 'rental_start_time'.
    try:
        transactions_df = transactions_df.withColumn("rental_start_time", to_timestamp("rental_start_time")) \
                                         .withColumn("rental_end_time", to_timestamp("rental_end_time"))
        transactions_df = transactions_df.withColumn("rental_duration_hours",
            (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600)
        transactions_df = transactions_df.withColumn("rental_date", to_date("rental_start_time"))
        print("INFO: Successfully preprocessed rental transactions data (timestamp conversion, duration, and date calculation).")
        # You might want to add data validation here, e.g., filter out records where rental_duration_hours is negative
        # transactions_df = transactions_df.filter(col("rental_duration_hours") >= 0)
    except Exception as e:
        print(f"ERROR: Failed during data preprocessing (timestamp conversion or duration/date calculation): {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 5. Metrics 1: Daily Metrics
    # ---------------------------------------
    # Group transactions by 'rental_date' to calculate daily aggregates:
    # - total_transactions: Count of all transactions per day
    # - total_revenue: Sum of 'total_amount' per day
    try:
        daily_metrics = transactions_df.groupBy("rental_date").agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("total_revenue")
        )
        print("INFO: Calculated daily transaction metrics.")
        # Show a sample of the results for verification
        daily_metrics.show(5)
    except Exception as e:
        print(f"ERROR: Failed during calculation of daily metrics: {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 6. Metrics 2: User Metrics
    # ---------------------------------------
    # Group transactions by 'user_id' to calculate user-specific performance:
    # - user_total_transactions: Total number of transactions per user
    # - user_total_spent: Total amount spent by each user
    # - user_total_hours: Total rental hours per user
    # - user_max_transaction: Maximum single transaction amount for a user
    # - user_min_transaction: Minimum single transaction amount for a user
    # - user_avg_transaction: Average transaction amount for a user
    try:
        user_metrics = transactions_df.groupBy("user_id").agg(
            count("*").alias("user_total_transactions"),
            sum("total_amount").alias("user_total_spent"),
            sum("rental_duration_hours").alias("user_total_hours"),
            max("total_amount").alias("user_max_transaction"),
            min("total_amount").alias("user_min_transaction"),
            avg("total_amount").alias("user_avg_transaction")
        )
        print("INFO: Calculated user-specific transaction metrics.")
        # Show a sample of the results for verification
        user_metrics.show(5)
    except Exception as e:
        print(f"ERROR: Failed during calculation of user metrics: {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 7. Join with user info
    # ---------------------------------------
    # Join the calculated user metrics with the 'users_df' to enrich with user details
    # (e.g., first_name, last_name, email).
    try:
        user_metrics = user_metrics.join(users_df, on="user_id", how="left")
        print("INFO: Joined user metrics with user details.")
        # Show a sample of the joined results
        user_metrics.show(5)
    except Exception as e:
        print(f"ERROR: Failed during joining user metrics with user details: {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 8. Write Outputs to Parquet in S3
    # ---------------------------------------
    # Write the calculated daily metrics and user metrics to S3 in Parquet format.
    # 'overwrite' mode will replace the output directory if it already exists.
    try:
        # Output for daily metrics
        daily_metrics_output_path = f"{output_path}daily_metrics"
        daily_metrics.write.mode("overwrite").parquet(daily_metrics_output_path)
        print(f"INFO: Successfully saved daily metrics to: {daily_metrics_output_path}")

        # Output for user metrics
        user_metrics_output_path = f"{output_path}user_metrics"
        user_metrics.write.mode("overwrite").parquet(user_metrics_output_path)
        print(f"INFO: Successfully saved user metrics to: {user_metrics_output_path}")

        print("✅ Job 2 Completed Successfully.")
    except Exception as e:
        print(f"ERROR: Failed to save outputs to S3: {e}")
        spark.stop()
        sys.exit(1)

    # ---------------------------------------
    # 9. Stop Spark session
    # ---------------------------------------
    spark.stop()
    print("INFO: Spark session stopped.")