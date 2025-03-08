import os
import pandas as pd
import findspark
from pyspark.sql import SparkSession, DataFrame
from datasets import load_dataset
from datetime import datetime
from logger import log_error, log_info


def init_spark(hadoop_home) -> SparkSession:
    """
    Initialize and return a Spark session with specific configurations.
    
    Returns:
        SparkSession: Configured Spark session instance.
    """
    try:
        findspark.init()
        spark = SparkSession.builder \
            .appName("AGNewsProcessing") \
            .config("spark.hadoop.fs.permissions.umask-mode", "007") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.hadoop.hadoop.home.dir", hadoop_home) \
            .getOrCreate()
        log_info("Spark session initialized successfully.")
        return spark
    
    except Exception as e:
        log_error(f"Error initializing Spark session: {e}")
        raise

def load_data(DATA_DIR, SPLIT, CSV_OUTPUT_DIR, CSV_FILENAME) -> None:
    """
    Load the dataset,
    and save it as a CSV file in the `data/` directory.
    """
    try:
        dataset = load_dataset(DATA_DIR, split=SPLIT)
        df = pd.DataFrame(dataset)

        os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
        csv_path = os.path.join(CSV_OUTPUT_DIR, CSV_FILENAME)
        df.to_csv(csv_path, index=False)

        log_info("Test data CSV file successfully written to 'data/' directory.")

    except Exception as e:
        log_error(f"Error loading and saving AG News dataset: {e}")
        raise

def save_to_parquet(df: DataFrame, filename_prefix: str, PARQUET_OUTPUT_DIR: str) -> None:
    """
    Save a given PySpark DataFrame to a Parquet file with a timestamped filename.
    
    Args:
        df (DataFrame): The PySpark DataFrame to save.
        filename_prefix (str): Prefix for the output filename.
    """
    try:
        date_str = datetime.now().strftime("%Y%m%d")
        outputs_dir = os.path.abspath(PARQUET_OUTPUT_DIR)
        os.makedirs(outputs_dir, exist_ok=True)

        filename = f"{filename_prefix}_{date_str}.parquet"
        parquet_path = os.path.join(outputs_dir, filename)

        # Overwrite if file exists
        df.write.mode("overwrite").parquet(parquet_path)
        log_info(f"File saved: {parquet_path}")

    except Exception as e:
        log_error(f"Error saving DataFrame to Parquet: {e}")
        raise
