import boto3
import pandas as pd
from io import StringIO
import logging
from typing import Optional, Union
from pandas import DataFrame


logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def load_data_from_s3(bucket_name: str, key: str) -> Optional[DataFrame]:
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
    
def handle_missing_values(df: DataFrame, window_size: Union[int, str]) -> DataFrame:
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
    

def save_data_to_s3(df: DataFrame, bucket_name: str, key: str) -> None:
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