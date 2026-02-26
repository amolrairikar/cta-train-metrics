"""
Step definitions for E2E tests.
"""

import json
import os
import time

import boto3
import requests
from behave import given, then, when, use_step_matcher
from behave.runner import Context
from dotenv import load_dotenv

from lambdas.gtfs_data_fetch.main import update_last_modified_time

load_dotenv()

use_step_matcher("re")

BUCKET_NAME = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"


def get_object_versions_under_s3_prefix(bucket: str, prefix: str) -> dict[str, str]:
    """
    Gets the current VersionId of all objects located in the given bucket and prefix.

    Args:
        bucket: The name of the S3 bucket.
        prefix: The name of the prefix within the S3 bucket.

    Returns:
        dict: A mapping of the object key to its VersionId
    """
    s3_client = boto3.client("s3")
    result = {}
    files = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )
    file_keys = [obj["Key"] for obj in files["Contents"]]
    for key in file_keys:
        obj_metadata = s3_client.head_object(
            Bucket=bucket,
            Key=key,
        )
        result[key] = obj_metadata["VersionId"]
    return result


@given(r"new GTFS data (is|is not) available")  # type: ignore[reportCallIssue]
def set_last_gtfs_fetch_parameter(context: Context, availability: str):
    """
    Set the value of gtfs_last_modified_time parameter in SSM Parameter Store
    to a past date if availability = "is", else if availability = "is not" then
    we do nothing.

    Args:
        context: The Behave context object.
        availability: Takes value "is" or "is not".
    """
    new_last_modified_time = "2026-01-01T00:00:00"
    if availability == "is":
        print(f"Updating gtfs_last_modified_time to {new_last_modified_time}")
        update_last_modified_time(last_modified_time=new_last_modified_time)
        print(f"Updated gtfs_last_modified_time to {new_last_modified_time}")
    else:
        print(
            "No update made to gtfs_last_modified_time to simulate no new available data."
        )


@given("existing GTFS raw data is present in S3")  # type: ignore[reportCallIssue]
def check_existing_gtfs_raw_data_s3(context: Context):
    """
    Checks S3 for the current versions of each GTFS raw data file. The versions
    are stored in Behave's context object to later assert whether versions changed
    or didn't change as expected.

    Args:
        context: The Behave context object.
    """
    context.gtfs_raw_file_versions = get_object_versions_under_s3_prefix(
        bucket=BUCKET_NAME,
        prefix="gtfs_data/",
    )
    assert len(context.gtfs_raw_file_versions) == 11, (
        f"Found {len(context.gtfs_processed_file_versions)} under gtfs_expected_cta_schedule/, expected 11"
    )


@given("existing GTFS processed schedule data is present in S3")  # type: ignore[reportCallIssue]
def check_existing_gtfs_processed_schedule_data_s3(context: Context):
    """
    Checks S3 for the current version of the GTFS processed schedule. The versions
    are stored in Behave's context object to later assert whether versions changed
    or didn't change as expected.

    Args:
        context: The Behave context object.
    """
    context.gtfs_processed_file_versions = get_object_versions_under_s3_prefix(
        bucket=BUCKET_NAME,
        prefix="gtfs_expected_cta_schedule/",
    )
    assert len(context.gtfs_processed_file_versions) == 1, (
        f"Found {len(context.gtfs_processed_file_versions)} under gtfs_expected_cta_schedule/, expected 1"
    )


@given("we are subscribed to orchestrator failure notifications")  # type: ignore[reportCallIssue]
def store_sqs_queue_url(context: Context):
    """
    Set the SQS queue URL used for end-to-end testing in the behave context object.

    Args:
        context: The Behave context object.
    """
    context.queue_url = f"https://sqs.us-east-1.amazonaws.com/{os.environ['ACCOUNT_NUMBER']}/e2e-testing-queue"


# Force 'parse' matcher since the matcher was initially set to 're'
use_step_matcher("parse")


@given("we update the {lambda_name} lambda to remove environment variables")  # type: ignore[reportCallIssue]
def update_lambda_environment_variable(context: Context, lambda_name: str):
    """
    Removes the ACCOUNT_NUMBER environment variable for `lambda_name` to force
    a failure and trigger SNS notification from Step Function execution.

    Args:
        context: The Behave context object.
        lambda_name: The name of the Lambda function to update
    """
    lambda_client = boto3.client("lambda")
    lambda_client.update_function_configuration(
        FunctionName=lambda_name,
        Environment={"Variables": {}},
    )
    waiter = lambda_client.get_waiter("function_updated_v2")
    print(f"Waiting for {lambda_name} to finish updating...")
    waiter.wait(FunctionName=lambda_name, WaiterConfig={"Delay": 2, "MaxAttempts": 10})
    print(f"Lambda function {lambda_name} completed updating.")
    final_config = lambda_client.get_function_configuration(FunctionName=lambda_name)
    print(final_config)
    assert final_config.get("Environment") is None, (
        "Failed to delete Lambda environment variable."
    )


@given("the CTA Train Locations API is available")  # type: ignore[reportCallIssue]
def check_cta_api_availability(context: Context):
    """
    Makes a test request to the CTA Train Locations API to validate the API service is up.

    Args:
        context: The Behave context object.
    """
    response = requests.get(
        url="http://lapi.transitchicago.com/api/1.0/ttpositions.aspx",
        params={
            "rt": "g",
            "key": os.environ["API_KEY"],
            "outputType": "JSON",
        },
        timeout=10,
    )
    response.raise_for_status()
    assert response.status_code == 200, (
        f"API request was successfull but returned {response.status_code} code instead of 200 code."
    )


@when("we trigger the GTFS lambda orchestrator")  # type: ignore[reportCallIssue]
def trigger_step_function(context: Context):
    """
    Trigger the Step Function that orchestrates the GTFS data fetch and GTFS data processing.

    Args:
        context: The Behave context object.
    """
    sfn_client = boto3.client("stepfunctions")
    response = sfn_client.start_execution(
        stateMachineArn=f"arn:aws:states:us-east-1:{os.environ['ACCOUNT_NUMBER']}:stateMachine:gtfs-lambda-orchestrator"
    )
    context.sfn_execution_arn = response["executionArn"]
    assert response["executionArn"] is not None, (
        "Could not find execution ARN for triggered execution."
    )


@when("we trigger the {lambda_name} lambda function")  # type: ignore[reportCallIssue]
def trigger_lambda(context: Context, lambda_name: str):
    """
    Trigger the lambda function with `lambda_name` and wait for it to complete.

    Args:
        context: The Behave context object.
        lambda_name: The name of the Lambda function to trigger.
    """
    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps({"test": "event"}).encode("utf-8"),
    )

    # Store response for later assertions
    context.response = response


@then("the orchestrator will have status {expected_status}")  # type: ignore[reportCallIssue]
def check_sfn_execution_status(context: Context, expected_status: str):
    """
    Poll the Step Function execution status using the execution ARN stored in Behave's context
    until the execution completes and assert that it matches `status`.

    Args:
        context: The Behave context object.
        expected_status: The expected status of the Step Function execution.
    """
    max_attempts = 20
    retry_interval = 10  # seconds
    attempts = 0
    sfn_client = boto3.client("stepfunctions")
    final_statuses = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]
    status = ""
    while attempts < max_attempts:
        response = sfn_client.describe_execution(executionArn=context.sfn_execution_arn)
        status = response["status"]
        if status in final_statuses:
            assert status == expected_status, (
                f"Step Function final status was {status} instead of {expected_status}"
            )
            return
        time.sleep(retry_interval)
        attempts += 1
        print(f"Status after attempt {attempts}: {status}")
    total_wait_time = max_attempts * retry_interval
    assert False, (
        f"Step Function polling timed out after {total_wait_time} seconds. "
        f"Last known status was: {status}"
    )


# Force 're' matcher since the matcher was updated to 'parse'
use_step_matcher("re")


@then(r"new GTFS raw data files (will|will not) be created in S3")  # type: ignore[reportCallIssue]
def check_for_new_gtfs_raw_data(context: Context, new_file_status: str):
    """
    Checks S3 for the current versions of each GTFS raw data file. The versions
    are compared with the versions stored in Behave's context object to assert
    whether versions changed or didn't change as expected.

    Args:
        context: The Behave context object.
        new_file_status: Takes value "will" or "will not".
    """
    new_file_versions = get_object_versions_under_s3_prefix(
        bucket=BUCKET_NAME,
        prefix="gtfs_data/",
    )
    if new_file_status == "will":
        for file, version in new_file_versions.items():
            assert context.gtfs_raw_file_versions[file] != version, (
                f"File {file} had same version ID before and after processing."
            )
    else:
        for file, version in new_file_versions.items():
            assert context.gtfs_raw_file_versions[file] == version, (
                f"File {file} had different version ID before and after processing."
            )


@then(r"new GTFS processed schedule data (will|will not) be created in S3")  # type: ignore[reportCallIssue]
def check_for_new_gtfs_processed_schedule_data(context: Context, new_file_status: str):
    """
    Checks S3 for the current version of the GTFS processed schedule data. The version
    is compared with the version stored in Behave's context object to assert
    whether the version changed or didn't change as expected.

    Args:
        context: The Behave context object.
        new_file_status: Takes value "will" or "will not".
    """
    new_file_versions = get_object_versions_under_s3_prefix(
        bucket=BUCKET_NAME,
        prefix="gtfs_expected_cta_schedule/",
    )
    if new_file_status == "will":
        for file, version in new_file_versions.items():
            assert context.gtfs_processed_file_versions[file] != version, (
                f"File {file} had same version ID before and after processing."
            )
    else:
        for file, version in new_file_versions.items():
            assert context.gtfs_processed_file_versions[file] == version, (
                f"File {file} had different version ID before and after processing."
            )


@then("an error notification will be sent")  # type: ignore[reportCallIssue]
def poll_sqs_queue(context: Context):
    """
    Polls the SQS queue configured to receive SNS messsages about Step Function failures
    to verify that a failure message was sent. If polling times out, an assertion error
    will be raised.

    Args:
        context: The Behave context object.
    """
    max_attempts = 12
    attempts = 0
    sqs_client = boto3.client("sqs")
    found_message = None
    while attempts < max_attempts:
        response = sqs_client.receive_message(
            QueueUrl=context.queue_url,
            WaitTimeSeconds=10,  # seconds to poll queue for
        )
        if response["Messages"]:
            found_message = response["Messages"][0]
            receipt_handle = response["Messages"][0]["ReceiptHandle"]
            sqs_client.delete_message(
                QueueUrl=context.queue_url, ReceiptHandle=receipt_handle
            )
            break
        attempts += 1
        print(f"Did not find messages on attempt {attempts}. Retrying...")
    assert found_message is not None, (
        "Could not find failure notification message in SQS queue"
    )

    # Assert the notification matches a GTFS error notification
    body = json.loads(found_message["Body"])
    expected_subject = "Alert: GTFS Pipeline Failure"
    actual_subject = body.get("Subject")
    assert actual_subject == expected_subject, (
        f"Expected subject '{expected_subject}', but got '{actual_subject}'"
    )


@then(r"the function will (succeed|fail)")  # type: ignore[reportCallIssue]
def check_lambda_status(context: Context, expected_result: str):
    """
    Check the response from the Lambda invocation to see if the function succeeded
    or failed as expected.

    Args:
        context: The Behave context object.
        expected_result: The expected result from the Lambda execution (`succeed` or `fail`).
    """
    lambda_response = json.loads(context.response["Payload"].read().decode("utf-8"))
    assert context.response["ResponseMetadata"]["HTTPStatusCode"] == 200
    if expected_result == "succeed":
        assert "FunctionError" not in context.response, (
            f"Lambda failed with error: {lambda_response}"
        )
        assert lambda_response["status"] == "success"
        assert lambda_response["count"] == 8
    else:
        assert "FunctionError" in context.response
        assert lambda_response["errorType"] == "KeyError"
