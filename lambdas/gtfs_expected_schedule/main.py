"""
Lambda function to construct expected CTA train schedule from GTFS data. The expected
schedule is uploaded to S3.
"""

import logging
import os
import sys

import boto3
import botocore.exceptions
import dotenv
import pandas as pd

# For debugging purposes
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)

GTFS_FILES = [
    "calendar.txt",
    "routes.txt",
    "stops.txt",
    "stop_times.txt",
    "trips.txt",
]
TRAIN_LINES = [
    "Red Line",
    "Purple Line",
    "Yellow Line",
    "Blue Line",
    "Pink Line",
    "Green Line",
    "Orange Line",
    "Brown Line",
]


def read_gtfs_data() -> dict[str, pd.DataFrame]:
    """
    Read GTFS data from S3.

    Returns:
        dict[str, pd.DataFrame]: Dictionary of dataframes keyed by filename.
    """
    bucket_name = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"
    prefix = "gtfs_data/"
    s3_client = boto3.client("s3")

    dataframes = {}

    for file in GTFS_FILES:
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=f"{prefix}{file}")
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error reading {file}: {e}")
            raise

        df_name = file.replace(".txt", "")
        dataframes[df_name] = pd.read_csv(response["Body"])
        logger.info(f"Successfully read {file}")

    return dataframes


def processs_gtfs_data(dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Process GTFS data to construct expected train schedule.

    Args:
        dataframes (dict[str, pd.DataFrame]): Dictionary of dataframes keyed by filename.

    Returns:
        pd.DataFrame: Processed dataframe with expected CTA train schedule.
    """
    # Step 0: Set up dataframes
    calendar_df = dataframes["calendar"]
    routes_df = dataframes["routes"]
    stops_df = dataframes["stops"]
    stop_times_df = dataframes["stop_times"]
    trips_df = dataframes["trips"]

    # Step 1: Select relevant columns from each dataframe and perform filtering
    calendar_df_filtered = calendar_df.drop(columns=["start_date", "end_date"])
    routes_df_filtered = (
        routes_df[["route_id", "route_long_name", "route_color"]]
        .query("route_long_name in @TRAIN_LINES")
        .reset_index()
    )
    stops_df_filtered = (
        stops_df[["stop_id", "stop_name"]]
        .query("stop_id >= 30000 and stop_id < 40000")
        .reset_index()
    )
    stop_times_df_filtered = (
        stop_times_df[
            ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]
        ]
        .query("stop_id >= 30000 and stop_id < 40000")
        .reset_index()
    )
    trips_df_filtered = trips_df[["route_id", "service_id", "trip_id", "direction_id"]]

    # Step 2: Join all dataframes together to construct expected schedule
    combined_df = pd.merge(
        left=routes_df_filtered, right=trips_df_filtered, on="route_id", how="inner"
    ).drop(columns=["index"])
    combined_df = pd.merge(
        left=combined_df, right=calendar_df_filtered, on="service_id", how="inner"
    )
    combined_df = pd.merge(
        left=combined_df, right=stop_times_df_filtered, on="trip_id", how="inner"
    ).drop(columns=["index"])
    combined_df = pd.merge(
        left=combined_df, right=stops_df_filtered, on="stop_id", how="inner"
    ).drop(columns=["index"])
    logger.info(f"Combined dataframe shape: {combined_df.shape}")
    return combined_df


def save_to_s3(df: pd.DataFrame):
    """
    Save a Pandas dataframe to S3.

    Args:
        df (pd.DataFrame): The dataframe to save.
    """
    s3_client = boto3.client("s3")
    bucket_name = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"
    logger.info("Writing file to S3...")
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key="gtfs_expected_cta_schedule.csv",
            Body=df.to_csv(index=False),
        )
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error writing to S3: {e}")
        raise
    logger.info("Successfully wrote file to S3")


def handler(event, context):
    """
    Lambda handler function to fetch GTFS data and store it in S3.

    Args:
        event: The event data that triggered the Lambda function.
        context: The context in which the Lambda function is running.
    """
    logger.info("Starting GTFS expected schedule Lambda execution.")
    logger.info("Event data: %s", event)
    logger.info("Context data: %s", context)

    # Only needed for local testing, will do nothing in Lambda environment
    dotenv.load_dotenv()

    # Read, process, and save to S3
    dataframes = read_gtfs_data()
    expected_schedule = processs_gtfs_data(dataframes=dataframes)
    save_to_s3(df=expected_schedule)
