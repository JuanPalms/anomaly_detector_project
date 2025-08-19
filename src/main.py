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

def run_script(script_name):
    """Runs a specified Python script using subprocess."""
    try:
        logger.info(f"Running script: {script_name}")
        subprocess.run(["python", script_name], check=True)
        logger.info(f"{script_name} executed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_name}: {e}")
        exit(1)

if __name__ == "__main__":
    logger.info("Starting the complete anomaly detection process.")
    run_script("data_prep.py")
    #run_script("model_training.py")
    #run_script("anomaly_detection.py")
    #logger.info("The complete anomaly detection process finished.")