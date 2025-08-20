"""
The script implements the data cleaning process for anomaly detection. It creates functions for data loading, processing, detection, and replacement of null values.
"""
import os
import logging
from outils import load_data_from_s3, handle_missing_values, save_data_to_s3
from typing import Union


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

def load_preprocess_and_save_data(
    bucket_name: str,
    input_key: str,
    output_key: str,
    window_size: Union[int, str],
) -> None:
    
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
        logger.exception(f"An unexpected error occurred during the data cleaning process: {e}")
        exit(1)
