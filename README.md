## Overview
This project demonstrates a comprehensive data pipeline using PySpark.
The pipeline includes data extraction, transformation, and loading (ETL) processes, with a focus on reusability and performance optimization.
It handles data for multiple markets (US, MX, CA etc).

## Project Structure
- `config/`: Configuration files.
- `etl/`: ETL scripts for data extraction, transformation, and loading.
- `jobs/`: Job scripts to run the ETL process.
- `utils/`: Utility scripts for Spark session management and data quality checks.
- `main.py`: Main script to execute the ETL job.

## Configuration
Update the configuration settings in `config/config.py` as needed.

## Run on the Local
1. Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

2. Run the ETL job:
    ```bash
    python main.py
    ```
## Run on the cluster
python3 setup.py sdist
gsutil cp dist/pyspark_data_pipeline-1.0.tar.gz gs://your-gcs-bucket/

gcloud dataproc jobs submit pyspark \
    --cluster=your-cluster \
    --region=your-region \
    --py-files=pyspark_etl_archetype-1.0.tar.gz \
    main.py \
    --properties spark.executor.memory=4g,spark.driver.memory=2g,spark.sql.shuffle.partitions=200# pyspark_etl_archetype
