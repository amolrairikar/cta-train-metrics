"""
Step definitions for GTFS data fetch feature tests.
"""

import os
import time

import boto3
from behave import given, then, when
from dotenv import load_dotenv

load_dotenv()


@given("the orchestrator is available")
def step_given_gtfs_sfn_orchestration_triggered(context):
    """
    Step that triggers the GTFS SFN orchestration.

    Args:
        context: The behave context object
    """
    sfn_client = boto3.client("stepfunctions")
    context.sfn_client = sfn_client
    response = sfn_client.describe_state_machine(
        stateMachineArn=f"arn:aws:states:us-east-1:{os.getenv('ACCOUNT_NUMBER')}:stateMachine:gtfs-lambda-orchestrator"
    )
    assert response is not None, "GTFS Lambda orchestrator not found"


@when("we run the GTFS Lambda orchestrator")
def step_when_run_gtfs_lambda_orchestrator(context):
    """
    Step that runs the GTFS Lambda orchestrator.

    Args:
        context: The behave context object
    """
    execution = context.sfn_client.start_execution(
        stateMachineArn=f"arn:aws:states:us-east-1:{os.getenv('ACCOUNT_NUMBER')}:stateMachine:gtfs-lambda-orchestrator"
    )
    execution_arn = execution["executionArn"]
    assert execution_arn is not None, (
        "Failed to start GTFS Lambda orchestrator execution"
    )
    context.execution_arn = execution_arn


@then("the step function should complete successfully")
def step_then_step_function_completed(context):
    """
    Step that verifies the step function completed successfully.

    Args:
        context: The behave context object
    """
    COMPLETED_STATUSES = ["SUCCEEDED", "TIMED_OUT", "ABORTED", "FAILED"]
    MAX_ATTEMPTS = 20
    current_status = "RUNNING"
    attempt = 0
    while current_status not in COMPLETED_STATUSES and attempt < MAX_ATTEMPTS:
        response = context.sfn_client.describe_execution(
            executionArn=context.execution_arn
        )
        current_status = response["status"]
        attempt += 1
        print(f"Attempt {attempt}: Status = {current_status}")
        time.sleep(10)
    assert current_status == "SUCCEEDED", (
        f"Step function did not complete successfully. Status: {current_status}"
    )
