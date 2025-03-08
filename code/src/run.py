import os
import argparse
import yaml
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from utils import init_spark, load_data
from data_processor import data_processor
import logger
from logger import log_error, log_info


def main() -> None:
    """
    Main function for processing news data using Spark.
    - Parses command-line arguments.
    - Loads configuration from a YAML file.
    - Initializes a Spark session.
    - Loads the dataset.
    - Processes the data based on the specified task.
    - Stops the Spark session after execution.
    """
    parser = argparse.ArgumentParser(description="News Data Processing")
    parser.add_argument("task", choices=["process_data", "process_data_all"], help="Task to run")
    parser.add_argument("--cfg", required=True, help="Config file path")
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument("--dirout", required=True, help="Output directory")

    args = parser.parse_args()
    logger.setup_logger(args.task)

    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    try:
        log_info("-"*50)
        log_info("Loading configuration file.")
        # Load configurations
        config_path = Path(args.cfg).resolve()
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        hadoop_home = os.getenv("HADOOP_HOME", config["hadoop_home"])

        CSV_OUTPUT_DIR = PROJECT_ROOT / config["csv"]["output_dir"]
        CSV_FILENAME = config["csv"]["filename"]
        CSV_FULL_PATH = PROJECT_ROOT / config["csv"]["full_path"]
        CSV_TEXT_COLUMN = config["csv"]["text_column"]
        TARGET_WORDS = config["target_words"]
        HUGGINGFACE_DATA_DIR = config["huggingface_data"]["dir"]
        HUGGINGFACE_DATA_SPLIT = config["huggingface_data"]["split"]
        OUTPUT_PREFIX = config["parquet"]["output_prefix"]
        OUTPUT_PREFIX_ALL = config["parquet"]["output_prefix_all"]
        PARQUET_OUTPUT_DIR = PROJECT_ROOT / args.dirout
        
        log_info("Initializing Spark session.")
        spark: SparkSession = init_spark(hadoop_home)
        
        log_info("Loading AG News dataset.")
        load_data(HUGGINGFACE_DATA_DIR, HUGGINGFACE_DATA_SPLIT, CSV_OUTPUT_DIR, CSV_FILENAME)
        
        log_info("Reading dataset into Spark DataFrame.")
        df_spark: DataFrame = spark.read.csv(str(CSV_FULL_PATH), header=True, inferSchema=True)
        

        if args.task == "process_data":
            log_info(f"Processing task: {args.task}")
            data_processor(df_spark, args.task, CSV_TEXT_COLUMN, TARGET_WORDS, OUTPUT_PREFIX, str(PARQUET_OUTPUT_DIR))
            log_info("Data processing completed successfully.")

        elif args.task == "process_data_all":
            log_info(f"Processing task: {args.task}")
            data_processor(df_spark, args.task, CSV_TEXT_COLUMN, TARGET_WORDS, OUTPUT_PREFIX_ALL, str(PARQUET_OUTPUT_DIR))
            log_info("Data processing completed successfully.")

    
    except Exception as e:
        log_error(f"Error in data processing pipeline: {e}")
        raise
    
    finally:
        log_info("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main()
