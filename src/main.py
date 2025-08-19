"""
This script implements the complete anomaly detection process through the use of various modules for cleaning, training, and evaluating results.
"""
import subprocess
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 


handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

try:
    S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
    S3_INPUT_KEY = os.environ['TRAIN_DATA_INPUT']
    S3_OUTPUT_KEY = os.environ['TRAIN_DATA_CLEAN']
    WINDOW_SIZE = int(os.environ['WINDOW_SIZE'])
except KeyError as e:
    logger.error(f"Missing environment variable: {e}")
    raise

def run_script(script_name):
    """Runs a specified Python script using subprocess, ensuring environment variables are passed."""
    try:
        logger.info(f"Running script: {script_name}")
        subprocess.run(["python", script_name], check=True, env=os.environ)
        logger.info(f"{script_name} executed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_name}: {e}")
        exit(1)

if __name__ == "__main__":
    logger.info("Starting the complete anomaly detection process.")
    run_script("src/data_prep.py")
    logger.info("Data preparation completed.")
    #run_script("model_training.py")
    #run_script("anomaly_detection.py")
    #logger.info("The complete anomaly detection process finished.")
