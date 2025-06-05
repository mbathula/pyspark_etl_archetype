from pyspark.sql import SparkSession
from utils.logger import get_logger

logger = get_logger(__name__)

def extract_data(spark: SparkSession, source_path: str):
    logger.info(f"Extracting data from {source_path}")
    return spark.read.format('parquet').load(source_path)