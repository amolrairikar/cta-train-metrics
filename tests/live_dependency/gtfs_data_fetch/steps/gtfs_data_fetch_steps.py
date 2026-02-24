"""
Step definitions for GTFS data fetch feature tests.
"""

import datetime
import os

import boto3
import requests
from behave import given, when, then
from behave import use_step_matcher
from dotenv import load_dotenv

from lambdas.gtfs_data_fetch.main import handler, get_last_modified_time

load_dotenv()

use_step_matcher("re")


def get_s3_object_versions(bucket_name, prefix="gtfs_data/"):
    """
    Utility function to list all objects in the specified S3 prefix and get their version IDs.

    Args:
        bucket_name (str): The S3 bucket name
        prefix (str): The prefix to filter objects (default: "gtfs_data/")

    Returns:
        dict: Dictionary mapping file keys to their version IDs
    """
    s3_client = boto3.client("s3")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        object_versions = {}

        if "Contents" in response:
            for obj in response["Contents"]:
                file_key = obj["Key"]
                # Get the object version ID
                version_response = s3_client.head_object(
                    Bucket=bucket_name, Key=file_key
                )
                version_id = version_response.get("VersionId", "null")
                object_versions[file_key] = version_id
        else:
            raise AssertionError(f"No files found in the {prefix} folder")

        return object_versions

    except Exception as e:
        raise AssertionError(f"Failed to get S3 object versions: {e}")


@given("we query GTFS data")
def step_query_gtfs_data(context):
    """
    Step that makes a GET request to the GTFS data URL and validates no error occurs.

    Args:
        context: The behave context object
    """
    gtfs_url = "https://www.transitchicago.com/downloads/sch_data/google_transit.zip"

    try:
        response = requests.get(gtfs_url)
        response.raise_for_status()
        context.gtfs_response = response
        context.gtfs_last_modified = response.headers.get("Last-Modified")
        if not context.gtfs_last_modified:
            raise AssertionError("GTFS Last-Modified header not found in response")
    except requests.exceptions.RequestException as e:
        raise AssertionError(f"Failed to query GTFS data: {e}")


@given(r"a new version of the data (is|is not) available")
def step_check_current_s3_versions(context, _availability):
    """
    Step that checks the S3 bucket and gets the object version for each file
    in the gtfs_data/ folder. Saves this mapping to the context for later verification.
    Also captures the original SSM parameter value for validation.

    Args:
        context: The behave context object
    """
    bucket_name = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"

    context.s3_object_versions = get_s3_object_versions(bucket_name)
    print(f"Current S3 object versions: {context.s3_object_versions}")

    # Capture the original parameter value for validation if no updates occur
    context.original_parameter_value = get_last_modified_time()
    print(f"Original SSM parameter value: {context.original_parameter_value}")


@when("we fetch the data")
def step_fetch_data(context):
    """
    Step that calls the lambda handler for GTFS data fetching.

    Args:
        context: The behave context object
    """
    mock_event = {"event": "test"}
    mock_context = type(
        "MockContext",
        (),
        {
            "function_name": "test-gtfs-fetch",
            "memory_limit_in_mb": 128,
            "invoked_function_arn": "arn:aws:lambda:us-east-1:123456789012:function:test-gtfs-fetch",
        },
    )()

    try:
        result = handler(mock_event, mock_context)
        context.lambda_result = result
        print(f"Lambda handler result: {result}")
    except Exception as e:
        raise AssertionError(f"Failed to fetch data using lambda handler: {e}")


@then(r"the data (should|should not) be saved to S3")
def step_data_saved_to_s3(context, should_save):
    """
    Step that validates whether the data was saved to S3 by checking object version IDs.
    If "should", validates that all files have new versions compared to the original.
    If "should not", validates that all files have the same versions as the original.

    Args:
        context: The behave context object
        should_save: String matching "should" or "should not" from the step pattern
    """
    bucket_name = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"
    current_versions = get_s3_object_versions(bucket_name)
    print(f"Current S3 object versions: {current_versions}")
    original_versions = context.s3_object_versions

    # Check that all original files still exist
    missing_files = set(original_versions.keys()) - set(current_versions.keys())
    if missing_files:
        raise AssertionError(
            f"The following files are missing from S3: {missing_files}"
        )

    # Determine validation logic based on should/should not
    expect_new_versions = should_save == "should"
    mismatched_files = []

    for file_key, original_version in original_versions.items():
        current_version = current_versions[file_key]

        if expect_new_versions:
            if current_version == original_version:
                mismatched_files.append(f"{file_key} (version: {original_version})")
        else:
            if current_version != original_version:
                mismatched_files.append(
                    f"{file_key} (original: {original_version}, current: {current_version})"
                )

    if mismatched_files:
        if expect_new_versions:
            raise AssertionError(
                f"The following files were not updated with new versions: {mismatched_files}"
            )
        else:
            raise AssertionError(
                f"The following files were unexpectedly updated with new versions: {mismatched_files}"
            )

    if expect_new_versions:
        print(
            f"Successfully validated that all {len(original_versions)} files were updated with new versions"
        )
    else:
        print(
            f"Successfully validated that all {len(original_versions)} files remained unchanged"
        )


@then(r"the last modified parameter (should|should not) be updated")
def step_last_modified_parameter_updated(context, should_update):
    """
    Step that validates whether the SSM parameter gtfs_last_modified_time has been updated.
    If "should", validates that the parameter matches the Last-Modified header from GTFS response.
    If "should not", validates that the parameter remained unchanged.

    Args:
        context: The behave context object
        should_update: String matching "should" or "should not" from the step pattern
    """
    current_parameter_value = get_last_modified_time()

    if should_update == "should":
        expected_last_modified = context.gtfs_last_modified

        # Convert the Last-Modified header to the same format as stored in SSM
        # Last-Modified format: "Mon, 23 Feb 2026 15:09:18 GMT"
        # SSM format: "2026-02-23T15:09:18"
        try:
            parsed_date = datetime.datetime.strptime(
                expected_last_modified, "%a, %d %b %Y %H:%M:%S %Z"
            )
            expected_parameter_value = parsed_date.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError as e:
            raise AssertionError(
                f"Failed to parse Last-Modified header '{expected_last_modified}': {e}"
            )

        # Validate the parameter was updated correctly
        if current_parameter_value != expected_parameter_value:
            raise AssertionError(
                "SSM parameter gtfs_last_modified_time was not updated correctly. "
                f"Expected: {expected_parameter_value}, Actual: {current_parameter_value}"
            )

        print("Successfully validated SSM parameter was updated to the correct value")
    else:
        if not hasattr(context, "original_parameter_value"):
            raise AssertionError(
                "Original parameter value not found in context. Make sure to capture it before lambda execution."
            )

        # Validate the parameter remained unchanged
        if current_parameter_value != context.original_parameter_value:
            raise AssertionError(
                "SSM parameter gtfs_last_modified_time was unexpectedly updated. "
                f"Expected: {context.original_parameter_value}, Actual: {current_parameter_value}"
            )

        print("Successfully validated SSM parameter remained unchanged")
