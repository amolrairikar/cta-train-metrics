"""
Module containing utility functions used by Streamlit app.
"""

import duckdb
import pandas as pd
import streamlit as st


@st.cache_data(ttl="1h")
def load_s3_parquet_data(s3_path: str) -> pd.DataFrame:
    """
    Queries an S3 location using DuckDB and caches the result for 1 hour.

    Args:
        s3_path: The S3 path. Can either be a glob pattern, e.g., 's3://my-bucket/data/*.parquet'
            to read a folder, or a file path, e.g., 's3://my-bucket/data/test.parquet'

    Returns:
        pd.DataFrame: A pandas dataframe with the loaded data
    """
    con = duckdb.connect(database=":memory:")

    # Configure S3 credentials (or use PROVIDER CREDENTIAL_CHAIN for auto-auth)
    con.execute("""
        CREATE OR REPLACE SECRET s3_creds (
            TYPE S3, 
            PROVIDER CREDENTIAL_CHAIN
        );
    """)

    query = f"SELECT * FROM read_parquet('{s3_path}')"
    df = con.execute(query).df()

    return df
