##import os

class Config:
    GCS_BUCKET = 'your-gcs-bucket' ##os.getenv('GCS_BUCKET', 'your-gcs-bucket')
    BIGQUERY_DATASET = 'your_bigquery_dataset' ##os.getenv('BIGQUERY_DATASET', 'your_bigquery_dataset')
    SPARK_APP_NAME = 'PySparkETL' ## os.getenv('SPARK_APP_NAME', 'PySparkETL')
    SPARK_MASTER = 'local[*]' ## os.getenv('SPARK_MASTER', 'local[*]')
    GCS_KEY_FILE = 'path/to/your/gcs/keyfile.json' ##os.getenv('GCS_KEY_FILE', 'path/to/your/gcs/keyfile.json')