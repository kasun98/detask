from utils import save_to_parquet
from logger import log_error, log_info
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split, lower, regexp_replace


def data_processor(df_spark: DataFrame, method: str, CSV_TEXT_COLUMN: str, TARGET_WORDS: list, 
                   OUTPUT_PREFIX: str, PARQUET_OUTPUT_DIR: str) -> None:
    """
    Processes text data by extracting and counting word occurrences.

    Args:
        df_spark (DataFrame): Input Spark DataFrame containing a text data column.
        method (str): Processing method. Either "TARGET_WORDS" or "ALL_WORDS".
        CSV_TEXT_COLUMN (str): Column contain the text data, 
        TARGET_WORDS (list): Specific words to count, 
        OUTPUT_PREFIX (str): Output dir of parquet file

    Raises:
        ValueError: If an unsupported processing method is provided.
    """
    try:
        log_info("Starting data processing pipeline.")

        # Text cleaning and word extraction
        words_df: DataFrame = df_spark.select(
            explode(
                split(
                    lower(regexp_replace(df_spark[CSV_TEXT_COLUMN], "[^a-zA-Z\\s]", "")), "\\s+"
                )
            ).alias("word")
        )

        # Remove empty/null rows
        cleaned_words_df: DataFrame = (
            words_df.filter(col("word").isNotNull())
                    .filter(col("word") != "")
                    .filter(col("word") != " ")
        )

        if method == "process_data":
            log_info(f"Filtering for target words: {TARGET_WORDS}")
            
            filtered_words: DataFrame = cleaned_words_df.filter(col("word").isin(TARGET_WORDS))

            log_info("Counting occurrences of target words.")
            word_counts: DataFrame = filtered_words.groupBy("word").count().orderBy("count", ascending=False)

            save_to_parquet(word_counts, OUTPUT_PREFIX, PARQUET_OUTPUT_DIR)

        elif method == "process_data_all":
            log_info("Counting occurrences of all words.")
            word_counts: DataFrame = cleaned_words_df.groupBy("word").count().orderBy("count", ascending=False)

            save_to_parquet(word_counts, OUTPUT_PREFIX, PARQUET_OUTPUT_DIR)

        else:
            raise ValueError(f"Invalid method '{method}'. Use 'process_data' or 'process_data_all'.")

    except Exception as e:

        log_error(f"Error during data processing: {e}")
        raise