"""
Lambda function to fetch train location data for each CTA train line. The API response data
is sent to Firehose for batch loading to S3.
"""

import concurrent.futures
import datetime
import json
import logging
import os
import sys
import time

import boto3
import botocore.exceptions
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)

API_BASE_URL = "http://lapi.transitchicago.com/api/1.0/ttpositions.aspx"


def fetch_cta_data(line: str, api_key: str, max_retries: int = 2) -> dict:
    """
    Function to fetch location data for a single train line with retry logic.

    Args:
        line: The name of the train line for the API request.
        api_key: The CTA API key to use for authenticated API requests.
        max_retries: Maximum number of retry attempts for failed requests.

    Returns:
        dict: The JSON API response.

    Raises:
        requests.RequestException: If all retry attempts fail.
    """
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(
                url=API_BASE_URL,
                params={
                    "rt": line,
                    "key": api_key,
                    "outputType": "JSON",
                },
                timeout=5,
            )
            response.raise_for_status()
            logger.info("Successfully fetched data for %s route", line)
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                logger.warning(
                    "Request error occurred for %s route (attempt %d/%d): %s. Retrying in %d seconds...",
                    line,
                    attempt + 1,
                    max_retries + 1,
                    str(e),
                    wait_time,
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    "Request error occurred for %s route after %d attempts: %s",
                    line,
                    max_retries + 1,
                    str(e),
                )
                raise


def write_to_firehose(payload: str):
    """
    Writes a payload to Kinesis Data Firehose.

    Args:
        payload: The payload to write to Kinesis Data Firehose.
    """
    firehose = boto3.client("firehose")
    DELIVERY_STREAM_NAME = "cta-train-locations-stream"
    try:
        firehose.put_record(
            DeliveryStreamName=DELIVERY_STREAM_NAME, Record={"Data": payload}
        )
        logger.info("Successfully sent payload to Firehose")
    except botocore.exceptions.ClientError as e:
        logger.error("Error occurred sending payload to Firehose: %s", str(e))
        raise


def handler(event, context):
    """
    Lambda handler function to fetch GTFS data and store it in S3.

    Args:
        event: The event data that triggered the Lambda function.
        context: The context in which the Lambda function is running.
    """
    logger.info("Starting train location fetch Lambda execution.")
    logger.info("Event data: %s", event)
    logger.info("Context data: %s", context)

    train_lines = [
        "blue",
        "brn",
        "g",
        "org",
        "p",
        "pink",
        "red",
        "y",
    ]
    current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

    all_data = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_url = {
            executor.submit(fetch_cta_data, line, os.environ["CTA_API_KEY"]): line
            for line in train_lines
        }

        for future in concurrent.futures.as_completed(future_to_url):
            try:
                data = future.result()
                all_data.append(data)
            except Exception as exc:
                logger.error("Error occurred: %s", str(exc))

    # Aggregate and send to Firehose as a single newline-delimited JSON record.
    # Firehose requires sending as a single newline-delimited JSON record or a bundled object.
    payload = json.dumps({"timestamp": current_time, "data": all_data}) + "\n"

    # Skip Firehose publish for test functions
    if not context.function_name.endswith("-test"):
        write_to_firehose(payload=payload)
    else:
        logger.info("Test function detected, skipping Firehose publish")

    return {"status": "success", "count": len(all_data)}
