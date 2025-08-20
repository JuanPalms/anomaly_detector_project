import pandas as pd
from outils import load_data_from_s3, handle_missing_values, save_data_to_s3

def test_load_data_from_s3(s3_mock):
    df = load_data_from_s3("anomaly-detection-test", "sensor_data_train.csv")
    assert list(df.columns) == ["value"]
    assert str(df.index.__class__.__name__) == "DatetimeIndex"
    assert pd.isna(df.iloc[1]["value"])

def test_handle_missing_values(s3_mock):
    df = load_data_from_s3("anomaly-detection-test", "sensor_data_train.csv")
    cleaned = handle_missing_values(df.copy(), window_size=2)
    assert not pd.isna(cleaned["value"]).any()
    assert abs(cleaned.iloc[1]["value"] - 1.0) < 1e-9

def test_save_data_to_s3(s3_mock):
    df = load_data_from_s3("anomaly-detection-test", "sensor_data_train.csv")
    cleaned = handle_missing_values(df.copy(), window_size=2)
    save_data_to_s3(cleaned, "anomaly-detection-test", "out.csv")
    df2 = load_data_from_s3("anomaly-detection-test", "out.csv")
    assert not df2["value"].isna().any()

