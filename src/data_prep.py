"""
The script implements the data cleaning process for anomaly detection. It creates functions for data loading, processing, detection, and replacement of null values.
"""
import os
import pandas as pd
import boto3
from io import StringIO
import logging
import yaml
from pathlib import Path

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_INPUT_KEY   = os.environ['TRAIN_DATA_INPUT']
S3_OUTPUT_KEY  = os.environ['TRAIN_DATA_CLEAN']
WINDOW_SIZE    = os.environ['WINDOW_SIZE']


def load_data_from_s3(bucket_name, key):
    """Loads data from a CSV file in S3 into a Pandas DataFrame."""
    s3 = boto3.client('s3')
    try:
        logger.info(f"Loading data from s3://{bucket_name}/{key}")
        response = s3.get_object(Bucket=bucket_name, Key=key)
        csv_data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        logger.info(f"Data loaded successfully from s3://{bucket_name}/{key}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from S3: {e}", exc_info=True)
        return None

def handle_missing_values(df, window_size):
    """Replaces missing values in the 'value' column with a rolling mean."""
    try:
        logger.info(f"Handling missing values using rolling mean with window size {window_size}")

        ws = int(window_size) if str(window_size).isdigit() else window_size

        df = df.sort_index()

        if isinstance(ws, int):
            rolling_mean_vals = (
                pd.Series(df['value'].to_numpy())
                  .rolling(window=ws, min_periods=1)
                  .mean()
                  .to_numpy()
            )
            rolling_mean = pd.Series(rolling_mean_vals, index=df.index)
        else:
            rolling_mean = df['value'].rolling(window=ws, min_periods=1).mean()

        df['value'] = df['value'].fillna(rolling_mean)
        df['value'] = df['value'].fillna(df['value'].mean())
        logger.info("Missing values handled successfully.")
        return df
    except Exception as e:
        logger.error(f"Error handling missing values: {e}", exc_info=True)
        return df


def save_data_to_s3(df, bucket_name, key):
    """Saves a Pandas DataFrame to a CSV file in S3."""
    s3 = boto3.client('s3')
    try:
        logger.info(f"Saving data to s3://{bucket_name}/{key}")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"Data saved successfully to s3://{bucket_name}/{key}")
    except Exception as e:
        logger.error(f"Error saving data to S3: {e}", exc_info=True)

def load_preprocess_and_save_data(bucket_name, input_key, output_key, window_size):
    """Loads data from S3, preprocesses it, and saves it back to S3."""
    logger.info("Starting data loading, preprocessing, and saving process.")
    df = load_data_from_s3(bucket_name, input_key)
    if df is None:
        logger.error("Data loading failed. Aborting.")
        return

    df = handle_missing_values(df, window_size)
    save_data_to_s3(df, bucket_name, output_key)
    logger.info("Data loading, preprocessing, and saving process completed.")

if __name__ == "__main__":
    try:
        bucket_name = S3_BUCKET_NAME
        input_key = S3_INPUT_KEY
        output_key = S3_OUTPUT_KEY
        window_size = WINDOW_SIZE

        logger.info("Starting data cleaning process.")
        load_preprocess_and_save_data(bucket_name, input_key, output_key, window_size)
        logger.info("Data cleaning process completed successfully.")

    except Exception as e:
        logger.exception("An unexpected error occurred during the data cleaning process.")
        exit(1)
