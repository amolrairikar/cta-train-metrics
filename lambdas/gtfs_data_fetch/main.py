"""
Lambda function to fetch GTFS data from the CTA and store it in S3.

This function is triggered by an EventBridge rule every night at midnight CST.
It fetches CTA's GTFS data from https://www.transitchicago.com/downloads/sch_data/ and
uploads all files from the zip to an S3 bucket
"""

import datetime
import io
import logging
import os
import sys
import zipfile

import boto3
import botocore.exceptions
import dotenv
import requests


logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)


def get_last_modified_time() -> str:
    """
    Fetch the last modified time of the GTFS data from Parameter Store.

    Returns:
        str: The last modified time as a string in ISO format.
    """
    ssm_client = boto3.client("ssm")
    try:
        response = ssm_client.get_parameter(
            Name="gtfs_last_modified_time", WithDecryption=False
        )
        return response["Parameter"]["Value"]
    except botocore.exceptions.ClientError as e:
        logger.error(
            "Failed to retrieve last modified time from Parameter Store: %s", e
        )
        raise e


def update_last_modified_time(last_modified_time: str):
    """
    Update the last modified time of the GTFS data in Parameter Store.

    Args:
        last_modified_time (str): The last modified time to store, in ISO format.
    """
    ssm_client = boto3.client("ssm")
    try:
        ssm_client.put_parameter(
            Name="gtfs_last_modified_time",
            Value=last_modified_time,
            Type="String",
            Overwrite=True,
        )
        logger.info("Successfully updated last modified time in Parameter Store.")
    except botocore.exceptions.ClientError as e:
        logger.error("Failed to update last modified time in Parameter Store: %s", e)
        raise e


def upload_gtfs_zip_to_s3(bucket_name: str, response: requests.Response):
    """
    Upload the contents of the GTFS zip file to S3.

    Args:
        bucket_name (str): The name of the S3 bucket to upload files to.
        response (requests.Response): The HTTP response containing the raw GTFS zip file data.
    """
    s3_client = boto3.client("s3")

    # Use BytesIO to handle the stream in memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        for file_info in zip_ref.infolist():
            logger.info("Processing file: %s", file_info.filename)
            # Skip directories
            if file_info.is_dir():
                logger.info("%s is a directory, skipping...", file_info.filename)
                continue

            # Read the file content from the zip and upload to S3
            with zip_ref.open(file_info) as file_content:
                s3_key = f"gtfs_data/{file_info.filename}"
                logger.info(
                    "Uploading %s to s3://%s/%s",
                    file_info.filename,
                    bucket_name,
                    s3_key,
                )
                try:
                    s3_client.upload_fileobj(
                        Fileobj=file_content,
                        Bucket=bucket_name,
                        Key=s3_key,
                    )
                except botocore.exceptions.ClientError as e:
                    logger.error(
                        "Error uploading %s to S3: %s", file_info.filename, str(e)
                    )
                    continue


def handler(event, context) -> dict[str, str]:
    """
    Lambda handler function to fetch GTFS data and store it in S3.

    Args:
        event: The event data that triggered the Lambda function.
        context: The context in which the Lambda function is running.

    Returns:
        dict: A response indicating the success of the operation.
    """
    logger.info("Starting GTFS data fetch Lambda execution.")
    logger.info("Event data: %s", event)
    logger.info("Context data: %s", context)

    # Only needed for local testing, will do nothing in Lambda environment
    dotenv.load_dotenv()

    bucket_name = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"

    logger.info("Making request to fetch GTFS data from CTA.")
    response = requests.get(
        "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"
    )
    response.raise_for_status()
    logger.info("Successfully fetched GTFS data from CTA.")

    # Get the last modified date and check if it has been updated since last fetch
    last_modified = response.headers.get("Last-Modified")
    if not last_modified:
        logger.error("Last-Modified header not found in the response")
        raise ValueError("Last-Modified header not found in the response")
    last_modified_dt = datetime.datetime.strptime(
        last_modified, "%a, %d %b %Y %H:%M:%S %Z"
    )
    stored_last_modified = get_last_modified_time()

    # If updated, write the GTFS data to S3 and update the last modified time in Parameter Store
    if (
        datetime.datetime.strptime(stored_last_modified, "%Y-%m-%dT%H:%M:%S")
        >= last_modified_dt
    ):
        logger.info("GTFS data has not been updated since last fetch. No action taken.")
        return {
            "status": "no_update",
            "message": "GTFS data has not been updated since last fetch.",
        }
    else:
        logger.info(
            "GTFS data has been updated since last fetch. Writing updated GTFS data to S3."
        )
        upload_gtfs_zip_to_s3(bucket_name=bucket_name, response=response)
        logger.info("Successfully wrote updated GTFS data to S3.")
        logger.info("Updating GTFS data last modified time in Parameter Store.")
        update_last_modified_time(last_modified_dt.strftime("%Y-%m-%dT%H:%M:%S"))
        logger.info(
            "Successfully updated GTFS data last modified time in Parameter Store."
        )
        return {
            "status": "updated",
            "message": "GTFS data has been updated and stored in S3.",
        }
