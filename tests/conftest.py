import boto3
import pytest
from moto import mock_aws

TEST_CSV = "timestamp,value\n2025-01-01 00:00:00,1\n2025-01-01 00:01:00,\n2025-01-01 00:02:00,3\n"

@pytest.fixture()
def s3_env(monkeypatch):
    monkeypatch.setenv("S3_BUCKET_NAME", "anomaly-detection-test")
    monkeypatch.setenv("TRAIN_DATA_INPUT", "sensor_data_train.csv")
    monkeypatch.setenv("TRAIN_DATA_CLEAN", "sensor_data_train_clean.csv")
    monkeypatch.setenv("WINDOW_SIZE", "2")

@pytest.fixture()
def s3_mock(s3_env):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="anomaly-detection-test")
        s3.put_object(Bucket="anomaly-detection-test", Key="sensor_data_train.csv", Body=TEST_CSV)
        yield s3

