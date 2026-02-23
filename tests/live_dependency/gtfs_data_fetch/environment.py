"""
Environment configuration for behave tests.
"""

from lambdas.gtfs_data_fetch.main import (
    get_last_modified_time,
    update_last_modified_time,
)


def before_all(context):
    """
    Before all hook that runs once before the test suite.

    The hook updates the SSM parameter gtfs_last_modified_time to 2026-01-01T01:00:00
    to ensure that the first test case (new GTFS data available) passes. It also saves
    the original value to the behave context for cleanup in after_all.

    Args:
        context: The behave context object
    """
    # Fetch the parameter gtfs_last_modified_time from SSM parameter store
    # and save the value to the behave context. We will use this value to restore
    # the original value in after_all.
    context.gtfs_last_modified_time = get_last_modified_time()
    print(f"Original GTFS last modified time: {context.gtfs_last_modified_time}")

    # Update the parameter gtfs_last_modified_time in SSM parameter store
    # to 2026-01-01T01:00:00 to guarantee that the first test case
    # (new GTFS data available) passes.
    update_last_modified_time("2026-01-01T01:00:00")
    print("Updated GTFS last modified time to 2026-01-01T01:00:00")


def after_all(context):
    """
    After all hook that runs once after the test suite.

    Args:
        context: The behave context object
    """
    # Restore the original value of the parameter gtfs_last_modified_time
    update_last_modified_time(context.gtfs_last_modified_time)
    print(f"Restored GTFS last modified time to {context.gtfs_last_modified_time}")
