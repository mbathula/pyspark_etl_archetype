from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from utils.logger import get_logger

logger = get_logger(__name__)

class BaseTransform:
    def __init__(self, df: DataFrame):
        self.df = df

    def transform_category(self):
        logger.info("Transforming category")
        self.df = self.df.withColumn('category', col('category').cast('string'))
        return self

    def transform_sub_category(self):
        logger.info("Transforming sub_category")
        self.df = self.df.withColumn('sub_category', col('sub_category').cast('string'))
        return self

    def transform_dept(self):
        logger.info("Transforming dept")
        self.df = self.df.withColumn('dept', col('dept').cast('string'))
        return self

    def get_transformed_df(self):
        return self.df