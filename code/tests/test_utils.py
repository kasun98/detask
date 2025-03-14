import os, sys
import pytest
import pandas as pd
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from src.utils import init_spark, load_data, save_to_parquet

#configurations
HADOOP_HOME = os.getenv("HADOOP_HOME", "${HADOOP_HOME}")
TEST_CSV_DIR = "code/tests/outputs"
TEST_PARQUET_DIR = "code/tests/outputs"
TEST_CSV_FILENAME = "test_data.csv"

@pytest.fixture(scope="session")
def spark():
    """Initialize a Spark session for testing."""
    return init_spark(HADOOP_HOME)

def test_spark_initialization(spark):
    """Test if the Spark session initializes correctly."""
    assert isinstance(spark, SparkSession), "Spark session was not initialized properly"

def test_load_data():
    """Test loading data and saving it as CSV."""
    os.makedirs(TEST_CSV_DIR, exist_ok=True)
    
    load_data("sh0416/ag_news", "test[:10]", TEST_CSV_DIR, TEST_CSV_FILENAME)
    
    csv_path = os.path.join(TEST_CSV_DIR, TEST_CSV_FILENAME)
    assert os.path.exists(csv_path), "CSV file was not created"
    
    df = pd.read_csv(csv_path)
    assert not df.empty, "CSV file is empty"

def test_save_to_parquet(spark):
    """Test saving a PySpark DataFrame to Parquet."""
    from pyspark.sql import Row
    csv_path = os.path.join(TEST_CSV_DIR, TEST_CSV_FILENAME)
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    save_to_parquet(df, "test_data", TEST_PARQUET_DIR)
    
    parquet_files = [f for f in os.listdir(TEST_PARQUET_DIR) if f.endswith(".parquet")]
    assert len(parquet_files) > 0, "No Parquet file was created"

