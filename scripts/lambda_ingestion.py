import json
import urllib.request
from datetime import datetime, timezone
import boto3


# AWS S3 client
s3 = boto3.client("s3")

# Configuration
API_URL = "https://customer-data-api-klz1.onrender.com/api/customer-data"
BUCKET_NAME = "aman-customer-data-lake-2026"
S3_PREFIX = "bronze/customer-data/"


def lambda_handler(event, context):

    try:
        # 1. Call the REST API
        print("Calling customer data API...")

        request = urllib.request.Request(
            API_URL,
            headers={
                "User-Agent": "AWS-Lambda-Customer-Data-Pipeline"
            }
        )

        with urllib.request.urlopen(request, timeout=30) as response:
            api_response = response.read().decode("utf-8")

        print("API response received successfully.")

        # 2. Confirm that we received data
        print(f"API response size: {len(api_response)} bytes")

        # 3. Create a timestamp for the raw snapshot
        timestamp = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H-%M-%SZ"
        )

        file_name = f"customer_data_{timestamp}.json"
        s3_key = f"{S3_PREFIX}{file_name}"

        # 4. Store the RAW API response directly in S3 Bronze
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=api_response.encode("utf-8"),
            ContentType="application/json"
        )

        print(
            f"Successfully uploaded data to "
            f"s3://{BUCKET_NAME}/{s3_key}"
        )

        # 5. Return success response
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Customer data successfully ingested",
                "s3_bucket": BUCKET_NAME,
                "s3_key": s3_key
            })
        }

    except Exception as e:

        print(f"ERROR: {str(e)}")

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Data ingestion failed",
                "error": str(e)
            })
        }