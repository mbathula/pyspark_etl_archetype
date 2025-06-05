from jobs.etl_job import run_etl_job

if __name__ == "__main__":
    source_path = "gs://your-gcs-bucket/source-data"
    target_path_us = "gs://your-gcs-bucket/target-data/us"
    target_path_mx = "gs://your-gcs-bucket/target-data/mx"
    target_path_ca = "gs://your-gcs-bucket/target-data/ca"

    run_etl_job(source_path, target_path_us, 'US')
    run_etl_job(source_path, target_path_mx, 'MX')
    run_etl_job(source_path, target_path_ca, 'CA')