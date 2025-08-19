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

#def _resolve_config_path() -> Path:
    # 1) Permite override con env
 #   env_path = os.getenv("CONFIG_PATH")
  #  if env_path and Path(env_path).is_file():
   #     return Path(env_path)

    # 2) Busca en el repo: ../config.yaml (desde src/)
    #here = Path(__file__).resolve()
    #repo_root_cfg = here.parent.parent / "config.yaml"
    #if repo_root_cfg.is_file():
     #   return repo_root_cfg

    # 3) Alternativa: src/config.yaml
    #src_cfg = here.parent / "config.yaml"
    #if src_cfg.is_file():
     #   return src_cfg

    # 4) Último intento: /app/config.yaml (típico WORKDIR)
    #docker_cfg = Path("/app/config.yaml")
    #if docker_cfg.is_file():
     #   return docker_cfg

    #raise FileNotFoundError("config.yaml not found in CONFIG_PATH, repo root, src/, or /app/")

#try:
 #   CONFIG_PATH = _resolve_config_path()
  #  logger.info(f"Using config at: {CONFIG_PATH}")
   # with open(CONFIG_PATH, "r") as f:
    #    config = yaml.safe_load(f)

    #S3_BUCKET_NAME = config["s3_bucket_name"]
    #S3_INPUT_KEY   = config["train_data_input"]
    #S3_OUTPUT_KEY  = config["train_data_clean"]
    #WINDOW_SIZE    = int(config["window_size"])
#except FileNotFoundError as e:
 #   logger.error(str(e))
  #  exit(1)


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
    """Replaces missing values in the 'value' column with the rolling mean."""
    try:
        logger.info(f"Handling missing values using rolling mean with window size {window_size}")
        rolling_mean = df['value'].rolling(window=window_size, min_periods=1).mean()
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
