from utils.spark_session import get_spark_session
from etl.extract import extract_data
from etl.load import load_data
from utils.data_quality import check_nulls, check_unique
from etl.us_transformations import USTransform
from etl.mx_transformations import MXTransform
from etl.ca_transformations import CATransform
from utils.logger import get_logger

logger = get_logger(__name__)


def run_etl_job(source_path: str, target_path: str, market: str):
    spark = get_spark_session()

    # Extract
    df = extract_data(spark, source_path)

    # Data Quality Checks
    check_nulls(df, ['bus_date', 'dept', 'category', 'sub_category'])
    check_unique(df, ['bus_date', 'dept', 'category', 'sub_category'])

    # Transform
    if market == 'US':
        transformer = USTransform(df)
    elif market == 'MX':
        transformer = MXTransform(df)
    elif market == 'CA':
        transformer = CATransform(df)
    else:
        raise ValueError(f"Unsupported market: {market}")

    transformed_df = transformer.transform_category() \
        .transform_sub_category() \
        .transform_dept() \
        .transform_currency() \
        .get_transformed_df()

    # Load
    load_data(transformed_df, target_path)

    spark.stop()
