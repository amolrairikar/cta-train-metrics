"""
Step definitions for GTFS expected schedule feature tests.
"""

import os

import boto3
from behave import given, when, then
from dotenv import load_dotenv

from lambdas.gtfs_expected_schedule.main import handler

load_dotenv()


@given("GTFS data is available")  # type: ignore[reportCallIssue]
def step_gtfs_data_available(context):
    """
    Step that validates GTFS data is available in S3.

    Args:
        context: The behave context object
    """
    REQUIRED_FILES = [
        "calendar.txt",
        "routes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    ]
    s3_client = boto3.client("s3")
    bucket_name = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"
    for file in REQUIRED_FILES:
        response = s3_client.head_object(Bucket=bucket_name, Key=f"gtfs_data/{file}")
        assert response["ContentLength"] > 0, f"File {file} is empty"


@when("we process the data")  # type: ignore[reportCallIssue]
def step_process_data(context):
    """
    Step that runs the lambda handler code.

    Args:
        context: The behave context object
    """
    handler(event={}, context={})


@then("the expected schedule should be saved to S3")  # type: ignore[reportCallIssue]
def step_expected_schedule_saved(context):
    """
    Step that validates the expected schedule is saved to S3.

    Args:
        context: The behave context object
    """
    bucket_name = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"
    s3_client = boto3.client("s3")
    response = s3_client.head_object(
        Bucket=bucket_name, Key="gtfs_expected_cta_schedule/20260106.parquet"
    )
    assert response["ContentLength"] > 0, "Expected schedule file is empty"
