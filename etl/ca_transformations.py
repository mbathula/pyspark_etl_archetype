from etl.base_transformations import BaseTransform
from pyspark.sql.functions import lit
from utils.logger import get_logger

logger = get_logger(__name__)

class CATransform(BaseTransform):
    def transform_currency(self):
        logger.info("Transforming currency for CA market")
        self.df = self.df.withColumn('currency', lit('CAD'))
        return self