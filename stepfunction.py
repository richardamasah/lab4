this worked but add error handlijg to it

{
  "Comment": "Full Pipeline: EMR Serverless + Glue + Athena Query",
  "StartAt": "Run EMR Job 1",
  "States": {
    "Run EMR Job 1": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:emrserverless:startJobRun",
      "Parameters": {
        "ApplicationId": "00ftgd0mglcs9o1d",
        "ExecutionRoleArn": "arn:aws:iam::992382846559:role/service-role/AmazonEMR-ExecutionRole-1750672410748",
        "ClientToken": "job1-run-001",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://lab4emr1/scripts/job1_vehicle_location_metrics.py",
            "EntryPointArguments": [
              "s3://lab4emr1/raw-data/rental_transactions/rental_transactions.csv",
              "s3://lab4emr1/raw-data/vehicles/vehicles.csv",
              "s3://lab4emr1/raw-data/locations/locations.csv",
              "s3://lab4emr1/processed/job1_output/"
            ]
          }
        }
      },
      "Next": "Run EMR Job 2"
    },
    "Run EMR Job 2": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:emrserverless:startJobRun",
      "Parameters": {
        "ApplicationId": "00ftgd0mglcs9o1d",
        "ExecutionRoleArn": "arn:aws:iam::992382846559:role/YOUR_EMR_ROLE_NAME",
        "ClientToken": "job2-run-001",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://lab4emr1/scripts/job2_user_transaction_analysis.py",
            "EntryPointArguments": [
              "s3://lab4emr1/raw-data/rental_transactions/rental_transactions.csv",
              "s3://lab4emr1/raw-data/users/users.csv",
              "s3://lab4emr1/processed/job2_output/"
            ]
          }
        }
      },
      "Next": "Start Glue Crawler 1"
    },
    "Start Glue Crawler 1": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {
        "Name": "rental-crawler1"
      },
      "Next": "Start Glue Crawler 2"
    },
    "Start Glue Crawler 2": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {
        "Name": "rental-crawler2"
      },
      "Next": "Run Athena Query"
    },
    "Run Athena Query": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:athena:startQueryExecution",
      "Parameters": {
        "QueryString": "SELECT location_name, total_revenue FROM rental.location_metrics ORDER BY total_revenue DESC LIMIT 1",
        "QueryExecutionContext": {
          "Database": "rental"
        },
        "ResultConfiguration": {
          "OutputLocation": "s3://lab4emr1/athena-results/"
        }
      },
      "Next": "Success"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}