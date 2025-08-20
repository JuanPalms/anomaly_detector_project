from data_prep import load_preprocess_and_save_data

def test_end_to_end(s3_env, s3_mock):
    load_preprocess_and_save_data(
        bucket_name="anomaly-detection-test",
        input_key="sensor_data_train.csv",
        output_key="sensor_data_train_clean.csv",
        window_size="2",
    )
    body = s3_mock.get_object(
        Bucket="anomaly-detection-test",
        Key="sensor_data_train_clean.csv"
    )["Body"].read().decode("utf-8")
    lines = body.strip().splitlines()
    assert "timestamp,value" in lines[0]
    assert lines[1].split(",")[1] != ""  # no NaN
