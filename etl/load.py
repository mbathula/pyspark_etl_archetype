from pyspark.sql import DataFrame
from utils.logger import get_logger

logger = get_logger(__name__)

def load_data(df: DataFrame, target_path: str):
    logger.info(f"Loading data to {target_path}")
    df.write.format('delta').mode('overwrite').save(target_path)