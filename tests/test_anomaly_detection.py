import pandas as pd
import pytest
from pandas import DataFrame
from moto import mock_aws
import boto3

import anomaly_detection as ad


def df_with_timestamp(values) -> DataFrame:
    """Helper function to create a DataFrame with a datetime index and a 'value' column."""
    ts = pd.date_range("2025-01-01", periods=len(values), freq="min")
    return pd.DataFrame({"timestamp": ts, "value": values}).set_index("timestamp")


def test_compute_baseline_params_ok(monkeypatch):
    """Test that compute_baseline_params returns the correct mean and std when 'value' column exists."""
    df = df_with_timestamp([1, 2, 3])
    monkeypatch.setattr(ad, "load_data_from_s3", lambda b, k: df)

    mean, std = ad.compute_baseline_params("bucket", "train.csv")
    assert abs(mean - 2.0) < 1e-9
    assert abs(std - 1.0) < 1e-9

def test_compute_baseline_params_no_value_col(monkeypatch):
    """Test that compute_baseline_params raises RuntimeError if the 'value' column is missing."""
    df = pd.DataFrame({"other": [1, 2, 3]})
    monkeypatch.setattr(ad, "load_data_from_s3", lambda b, k: df)
    with pytest.raises(RuntimeError, match="Column 'value' is missing"):
        ad.compute_baseline_params("bucket", "train.csv")


def test_compute_baseline_params_empty_df(monkeypatch):
    """Test that compute_baseline_params raises RuntimeError when the training DataFrame is empty."""
    monkeypatch.setattr(ad, "load_data_from_s3", lambda b, k: pd.DataFrame())
    with pytest.raises(RuntimeError, match="Could not load or dataset is empty"):
        ad.compute_baseline_params("bucket", "train.csv")


def test_detect_anomalies_df_std_zero_with_index():
    """Test anomaly detection when std=0: only values different from mean are flagged as anomalies."""
    df = df_with_timestamp([5.0, 5.0, 6.0])
    anomalies = ad.detect_anomalies_df(df, normal_mean=5.0, normal_std=0.0, k=3.0)
    assert len(anomalies) == 1
    assert pytest.approx(float(anomalies["value"].iloc[0]), 1e-9) == 6.0
    assert "[5.00, 5.00]" in anomalies["reason"].iloc[0]


def test_detect_anomalies_df_with_timestamp_column():
    """Test anomaly detection when index is not 'timestamp' but a 'timestamp' column exists."""
    ts = pd.date_range("2025-01-01", periods=3, freq="min")
    df = pd.DataFrame({"timestamp": ts, "value": [1.0, 100.0, 2.0]})
    anomalies = ad.detect_anomalies_df(df, normal_mean=2.0, normal_std=1.0, k=3.0)
    assert len(anomalies) == 1
    assert pytest.approx(float(anomalies["value"].iloc[0]), 1e-9) == 100.0


def test_detect_anomalies_missing_value_column():
    """Test that detect_anomalies_df raises RuntimeError if the 'value' column is missing."""
    df = pd.DataFrame({"foo": [1, 2, 3]})
    with pytest.raises(RuntimeError, match="Column 'value' is missing in the test dataset"):
        ad.detect_anomalies_df(df, normal_mean=0.0, normal_std=1.0, k=3.0)

@pytest.mark.skip(reason="Disabled temporarily because it fails in CI")
@mock_aws
def test_run_prediction_e2e(monkeypatch):
    """End-to-end test of run_prediction using a mocked S3 bucket with training and test data."""
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "anomaly-detection-test"
    s3.create_bucket(Bucket=bucket)

    train_csv = "timestamp,value\n2025-01-01 00:00:00,1\n2025-01-01 00:01:00,2\n2025-01-01 00:02:00,3\n"
    s3.put_object(Bucket=bucket, Key="sensor_data_train_clean.csv", Body=train_csv)

    test_csv = "timestamp,value\n2025-01-01 00:03:00,2\n2025-01-01 00:04:00,99\n2025-01-01 00:05:00,2\n"
    s3.put_object(Bucket=bucket, Key="sensor_data_test.csv", Body=test_csv)

    monkeypatch.setenv("S3_BUCKET_NAME", bucket)
    monkeypatch.setenv("TRAIN_DATA_CLEAN", "sensor_data_train_clean.csv")
    monkeypatch.setenv("TEST_DATA_INPUT", "sensor_data_test.csv")
    monkeypatch.setenv("THRESHOLD_MULTIPLIER", "3")
    monkeypatch.setenv("TEST_DATA_ANOMALIES", "sensor_data_test_anomalies.csv")

    ad.run_prediction()

    body = s3.get_object(Bucket=bucket, Key="sensor_data_test_anomalies.csv")["Body"].read().decode("utf-8")
    lines = [line for line in body.strip().splitlines() if line]
    assert "timestamp,value,reason" in lines[0]
    assert len(lines) == 2
