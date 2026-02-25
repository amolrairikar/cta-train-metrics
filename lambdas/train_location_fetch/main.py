"""
Lambda function to fetch train location data for each CTA train line. The API response data
is sent to Firehose for batch loading to S3.
"""

import concurrent.futures
import json
import logging
import os
import sys

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

API_KEY = os.environ["CTA_API_KEY"]
API_BASE_URL = "http://lapi.transitchicago.com/api/1.0/ttpositions.aspx"


def fetch_cta_data(line: str) -> dict:
    """
    Function to fetch location data for a single train line.

    Args:
        line: The name of the train line for the API request.

    Returns:
        dict: The JSON API response.
    """
    response = requests.get(
        url=API_BASE_URL,
        params={
            "rt": line,
            "key": API_KEY,
            "outputType": "JSON",
        },
        timeout=10,
    )
    response.raise_for_status()
    logger.info("Successfully fetched data for %s route", line)
    return response.json()


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

    all_data = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_url = {
            executor.submit(fetch_cta_data, line): line for line in train_lines
        }

        for future in concurrent.futures.as_completed(future_to_url):
            try:
                data = future.result()
                all_data.append(data)
            except Exception as exc:
                logger.error("Error occurred: %s", str(exc))

    # Aggregate and send to Firehose as a single newline-delimited JSON record.
    # Firehose requires sending as a single newline-delimited JSON record or a bundled object.
    payload = json.dumps({"timestamp": "...", "data": all_data}) + "\n"
    write_to_firehose(payload=payload)

    return {"status": "success", "count": len(all_data)}
