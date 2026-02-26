"""
Hooks that automatically execute before and after Behave tests have run.
"""

import os
from pathlib import Path

import boto3
from behave.runner import Context
from dotenv import load_dotenv

from lambdas.gtfs_data_fetch.main import (
    get_last_modified_time,
    update_last_modified_time,
)

script_dir = Path(__file__).resolve().parent
env_path = script_dir / "steps" / ".env"
load_dotenv(env_path)


def before_all(context: Context):
    """
    Hook that runs before Behave tests start.

    Args:
        context: The Behave context object.
    """
    # Store the current gtfs_last_modified_time so we can set the value back
    # to this after tests have completed running.
    context.current_last_modified_time = get_last_modified_time()
    print(
        f"Current value of gtfs_last_modified_time: {context.current_last_modified_time}"
    )


def after_all(context: Context):
    """
    Hook that runs after Behave tests have completed.

    Args:
        context: The Behave context object.
    """
    # Restore the original value for gtfs_last_modified_time
    update_last_modified_time(last_modified_time=context.current_last_modified_time)
    print(
        f"Reset gtfs_last_modified_time to original value of {context.current_last_modified_time}"
    )

    # Restore the original environment variable for the gtfs-data-fetch lambda
    lambda_client = boto3.client("lambda")
    lambda_client.update_function_configuration(
        FunctionName="gtfs-data-fetch",
        Environment={"Variables": {"ACCOUNT_NUMBER": os.environ["ACCOUNT_NUMBER"]}},
    )
