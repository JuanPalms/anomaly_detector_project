"""
This script loads the test dataset from S3, computes baseline statistics
(mean and standard deviation) from the cleaned training dataset,
and detects anomalies in the test dataset based on configurable thresholds.
The detected anomalies are then saved back to S3.
"""

import os
import logging
import pandas as pd
from outils import load_data_from_s3, save_data_to_s3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)

S3_BUCKET_NAME        = os.environ["S3_BUCKET_NAME"]
TRAIN_DATA_CLEAN_KEY  = os.getenv("TRAIN_DATA_CLEAN", os.getenv("TRAIN_DATA_INPUT"))
TEST_DATA_KEY         = os.getenv("TEST_DATA_CLEAN", os.getenv("TEST_DATA_INPUT"))
THRESHOLD_MULTIPLIER  = float(os.getenv("THRESHOLD_MULTIPLIER", "3"))
TEST_DATA_ANOMALIES   = os.getenv("TEST_DATA_ANOMALIES", "sensor_data_test_anomalies.csv")


def compute_baseline_params(bucket_name: str, train_key: str) -> tuple[float, float]:
    """
    Loads the cleaned training dataset and computes baseline parameters.

    Args:
        bucket_name (str): Name of the S3 bucket.
        train_key (str): Key of the cleaned training dataset in S3.

    Returns:
        tuple: Mean and standard deviation of the 'value' column.
    """
    df_train = load_data_from_s3(bucket_name, train_key)
    if df_train is None or df_train.empty:
        msg = f"Could not load or dataset is empty: s3://{bucket_name}/{train_key}"
        logger.error(msg)
        raise RuntimeError(msg)

    if "value" not in df_train.columns:
        raise RuntimeError("Column 'value' is missing in the training dataset.")

    series = pd.to_numeric(df_train["value"], errors="coerce").dropna()
    if series.empty:
        raise RuntimeError("Training dataset contains no valid 'value' entries.")

    mean = float(series.mean())
    std  = float(series.std(ddof=1))  # Sample standard deviation
    logger.info(f"Baseline -> mean={mean:.6f}, std={std:.6f}")
    return mean, std


def detect_anomalies_df(df: pd.DataFrame, normal_mean: float, normal_std: float, k: float) -> pd.DataFrame:
    """
    Detects anomalies in the test dataset based on mean and standard deviation.

    Args:
        df (pd.DataFrame): Test dataset containing a 'value' column.
        normal_mean (float): Mean of the training dataset.
        normal_std (float): Standard deviation of the training dataset.
        k (float): Threshold multiplier.

    Returns:
        pd.DataFrame: A DataFrame containing anomalies with timestamp, value, and reason.
    """
    if "value" not in df.columns:
        raise RuntimeError("Column 'value' is missing in the test dataset.")

    values = pd.to_numeric(df["value"], errors="coerce")

    if normal_std == 0.0:
        mask = values.ne(normal_mean)
        lower_bound = upper_bound = normal_mean
    else:
        lower_bound = normal_mean - k * normal_std
        upper_bound = normal_mean + k * normal_std
        mask = values.lt(lower_bound) | values.gt(upper_bound)

    anomalies = df.loc[mask, ["value"]].copy()

    if df.index.name != "timestamp":
        if "timestamp" in df.columns:
            df = df.set_index(pd.to_datetime(df["timestamp"]))
            anomalies = df.loc[mask, ["value"]].copy()
        else:
            logger.warning("No 'timestamp' index or column found; using the existing index.")

    anomalies = anomalies.reset_index().rename(columns={"index": "timestamp"})
    anomalies["reason"] = anomalies["value"].apply(
        lambda v: f"Value {float(v):.2f} is outside the normal range [{lower_bound:.2f}, {upper_bound:.2f}]"
    )

    logger.info(f"Detected anomalies: {len(anomalies)}")
    return anomalies


def run_prediction():
    """Runs the full anomaly detection pipeline for the test dataset."""
    logger.info("Start finding anomalies")

    mean, std = compute_baseline_params(S3_BUCKET_NAME, TRAIN_DATA_CLEAN_KEY)

    df_test = load_data_from_s3(S3_BUCKET_NAME, TEST_DATA_KEY)
    if df_test is None or df_test.empty:
        msg = f"Could not load or dataset is empty: s3://{S3_BUCKET_NAME}/{TEST_DATA_KEY}"
        logger.error(msg)
        raise RuntimeError(msg)

    anomalies_df = detect_anomalies_df(df_test, mean, std, THRESHOLD_MULTIPLIER)

    save_data_to_s3(anomalies_df, S3_BUCKET_NAME, TEST_DATA_ANOMALIES)
    logger.info(f"Anomalies saved to s3://{S3_BUCKET_NAME}/{TEST_DATA_ANOMALIES}")


if __name__ == "__main__":
    try:
        run_prediction()
    except Exception as e:
        logger.exception("Step 2: Prediction failed")
        raise
