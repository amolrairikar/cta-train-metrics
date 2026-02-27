import gzip
import json
import logging
import os
import sys
from io import BytesIO

import boto3
import botocore.exceptions
import dotenv
import pandas as pd

dotenv.load_dotenv()

logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)


def read_s3_partition(bucket_name: str, partition_path: str) -> list[BytesIO]:
    """
    Read all files from a specific partition path in S3.

    Args:
        bucket_name: The name of the S3 bucket.
        partition_path: The full partition path (e.g., 'cta-data/year=2026/month=02/day=27/')

    Returns:
        List of BytesIO objects containing file contents of each file in the partition.
    """
    s3_client = boto3.client("s3")
    file_objects = []

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=partition_path)
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error listing files in S3 bucket: {e}")
        return file_objects

    if "Contents" not in response:
        logger.info(f"No files found in partition path: {partition_path}")
        return file_objects

    total_available_files = len(response["Contents"])

    logger.info(
        f"Found {total_available_files} files in partition path: {partition_path}"
    )

    for obj in response["Contents"]:
        file_key = obj["Key"]
        logger.info(f"Downloading file: {file_key}")
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response["Body"].read()
            file_objects.append(BytesIO(file_content))
            logger.info(f"Successfully downloaded: {file_key}")
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error downloading {file_key}: {e}")
            continue

    logger.info(
        f"Successfully downloaded {len(file_objects)} of {total_available_files} files"
    )
    return file_objects


def write_df_to_s3(df: pd.DataFrame, bucket_name: str, key: str):
    """
    Write a Pandas dataframe to S3.

    Args:
        df: The pandas DataFrame to write to S3.
        bucket_name: The name of the S3 bucket.
        key: The key for the object in S3.
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=df.to_csv(index=False))
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error writing to S3: {e}")
        raise e


def extract_cta_data_from_s3(bucket_name: str, partition_path: str) -> pd.DataFrame:
    """
    Extract CTA train data from all gzipped JSON files in an S3 partition path.

    Args:
        bucket_name: The name of the S3 bucket.
        partition_path: The full partition path (e.g., 'cta-data/year=2026/month=02/day=27/')

    Returns:
        pandas DataFrame with flattened data from all files
    """
    file_objects = read_s3_partition(
        bucket_name=bucket_name, partition_path=partition_path
    )
    if not file_objects:
        logger.info("No files to process")
        return pd.DataFrame()

    all_records = []
    total_processed_lines = 0
    total_skipped_lines = 0
    files_processed = 0

    for file_idx, file_obj in enumerate(file_objects, 1):
        logger.info(f"Processing file {file_idx}/{len(file_objects)}...")
        try:
            # Reset file pointer to beginning
            file_obj.seek(0)
            with gzip.GzipFile(fileobj=file_obj, mode="rb") as gz_file:
                content = gz_file.read().decode("utf-8")
                records = []
                processed_lines = 0
                skipped_lines = 0

                for line_num, line in enumerate(content.splitlines(), 1):
                    line = line.strip()
                    if not line:
                        skipped_lines += 1
                        continue

                    try:
                        json_data = json.loads(line)
                        ingestion_timestamp = json_data.get("timestamp")
                        data_list = json_data.get("data", [])

                        for data_item in data_list:
                            ctatt = data_item.get("ctatt", {})
                            current_timestamp = ctatt.get("tmst")
                            error_code = ctatt.get("errCd")
                            error_number = ctatt.get("errNm")

                            routes = ctatt.get("route", [])
                            for route in routes:
                                route_name = route.get("@name")
                                trains = route.get("train", [])
                                for train in trains:
                                    record = {
                                        "ingestion_timestamp": ingestion_timestamp,
                                        "current_timestamp": current_timestamp,
                                        "error_code": error_code,
                                        "error_number": error_number,
                                        "route_name": route_name,
                                        "run_number": train.get("rn"),
                                        "destination_station_id": train.get("destSt"),
                                        "destination_station_name": train.get("destNm"),
                                        "train_direction": train.get("trDr"),
                                        "next_station_id": train.get("nextStaId"),
                                        "next_stop_id": train.get("nextStpId"),
                                        "next_station_name": train.get("nextStaNm"),
                                        "prediction_timestamp": train.get("prdt"),
                                        "predicted_arrival": train.get("arrT"),
                                        "is_approaching": train.get("isApp"),
                                        "is_delayed": train.get("isDly"),
                                    }
                                    records.append(record)
                                    processed_lines += 1

                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing line {line_num}: {e}")
                        skipped_lines += 1
                        continue
                    except Exception as e:
                        logger.error(f"Error processing line {line_num}: {e}")
                        skipped_lines += 1
                        continue

                logger.info(
                    f"File {file_idx}: Processed {processed_lines} out of {processed_lines + skipped_lines} records."
                )
                if skipped_lines > 0:
                    logger.info(
                        f"File {file_idx}: Skipped {skipped_lines} lines due to errors"
                    )

                all_records.extend(records)
                total_processed_lines += processed_lines
                total_skipped_lines += skipped_lines
                files_processed += 1

        except Exception as e:
            logger.error(f"Error processing file {file_idx}: {e}")
            continue

        finally:
            file_obj.close()

    logger.info(f"Files processed: {files_processed}/{len(file_objects)}")
    logger.info(f"Total records processed: {total_processed_lines}")
    if total_skipped_lines > 0:
        logger.info(f"Total lines skipped: {total_skipped_lines}")

    return pd.DataFrame(all_records)


def handler(event, context):
    """
    Lambda handler function to process raw CTA train location data and store it in S3.

    Args:
        event: The event data that triggered the Lambda function.
        context: The context in which the Lambda function is running.

    Returns:
        dict: A response indicating the success of the operation.
    """
    logger.info("Starting CTA data processing Lambda execution.")
    logger.info("Event data: %s", event)
    logger.info("Context data: %s", context)

    # Only needed for local testing, will do nothing in Lambda environment
    dotenv.load_dotenv()

    bucket_name = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"
    partition_path = "raw-api-data/success/year=2026/month=02/day=27/"

    logger.info("Extracting CTA train data from S3...")
    extract_cta_data_from_s3(bucket_name=bucket_name, partition_path=partition_path)
    # df = extract_cta_data_from_s3(bucket_name=bucket_name, partition_path=partition_path)

    logger.info("Writing CTA train data to S3...")
    # write_df_to_s3(df=df, bucket_name=bucket_name, key="cta_train_data.csv")

    return {
        "status": "success",
        "message": "CTA data processing complete.",
    }
