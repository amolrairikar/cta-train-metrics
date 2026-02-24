"""
Lambda function to construct expected CTA train schedule from GTFS data. The expected
schedule is uploaded to S3.
"""

import logging
import os
import sys

import duckdb
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)

# Only needed for local testing, will do nothing in Lambda environment
load_dotenv()


def get_db_connection() -> duckdb.DuckDBPyConnection:
    """
    Create DuckDB connection to S3.

    Returns:
        duckdb.DuckDBPyConnection: The DuckDB connection
    """
    con = duckdb.connect(database=":memory:")

    # Register the credential chain. This automatically looks for ~/.aws/credentials (local)
    # or execution role (Lambda/EC2)
    con.execute("""
        CREATE OR REPLACE SECRET s3_creds (
            TYPE S3, 
            PROVIDER CREDENTIAL_CHAIN
        );
    """)
    return con


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

    con = get_db_connection()
    bucket = f"{os.environ['ACCOUNT_NUMBER']}-cta-analytics-project"
    prefix = f"s3://{bucket}/gtfs_data/"

    # Read all input files
    files = ["calendar", "routes", "stops", "stop_times", "trips"]
    for f in files:
        logger.info("Reading %s.txt", f)
        con.execute(
            f"CREATE VIEW {f} AS SELECT * FROM read_csv_auto('{prefix}{f}.txt')"
        )
        logger.info("Successfully read %s.txt as view %s", f, f)

    # Query to join all data together
    query = """
        SELECT
            r.route_id,
            r.route_long_name,
            r.route_color,
            t.service_id,
            t.trip_id,
            t.direction,
            t.direction_id,
            c.monday,
            c.tuesday,
            c.wednesday,
            c.thursday,
            c.friday,
            c.saturday,
            c.sunday,
            c.start_date,
            c.end_date,
            st.arrival_time,
            st.departure_time,
            st.stop_id,
            st.stop_sequence,
            s.stop_name
        FROM routes r
        JOIN trips t ON r.route_id = t.route_id
        JOIN calendar c ON t.service_id = c.service_id
        JOIN stop_times st ON t.trip_id = st.trip_id
        JOIN stops s ON st.stop_id = s.stop_id
        WHERE r.route_long_name IN (
            'Red Line',
            'Purple Line',
            'Yellow Line',
            'Blue Line', 
            'Pink Line',
            'Green Line',
            'Orange Line',
            'Brown Line'
        )
        AND s.stop_id >= 30000 AND s.stop_id < 40000
    """

    # Query to get effective date
    result = con.execute("SELECT MIN(start_date) FROM calendar").fetchone()
    if not result or result[0] is None:
        raise ValueError("No start_date found")
    effective_date = result[0]

    # Write output as Parquet to S3
    output_path = f"s3://{bucket}/gtfs_expected_cta_schedule/{effective_date}.parquet"
    con.execute(f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)")
    logger.info("Successfully wrote output parquet file to S3")

    return {"status": "success", "effective_date": effective_date}
