import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

#path to the S3 bucket root
REQUIRED_FILES = [
    'raw-data/rental_transactions/rental_transactions.csv',
    'raw-data/vehicles/vehicles.csv',
    'raw-data/locations/locations.csv',
    'raw-data/users/users.csv'
]

def lambda_handler(event, context):
    """
    Validate presence of all required input files in the S3 raw-data folder.
    Used as a guardrail before running EMR Spark jobs via Step Functions.
    """
    bucket = "lab4emr1"
    missing_files = []

    logger.info("Starting validation for required input files in bucket: %s", bucket)

    for key in REQUIRED_FILES:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            logger.info("File exists: %s", key)
        except s3.exceptions.ClientError as e:
            logger.warning("Missing file: %s", key)
            missing_files.append(key)
        except Exception as ex:
            logger.error("Unexpected error checking %s: %s", key, str(ex))
            return {
                "valid": False,
                "error": f"Unexpected error: {str(ex)}"
            }

    if missing_files:
        logger.warning("Validation failed. Missing files: %s", missing_files)
        return {
            "valid": False,
            "missing_files": missing_files
        }

    logger.info("All required input files are present.")
    return {
        "valid": True
    }
