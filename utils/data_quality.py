from pyspark.sql import DataFrame

def check_nulls(df: DataFrame, columns: list):
    for column in columns:
        null_count = df.filter(df[column].isNull()).count()
        if null_count > 0:
            raise ValueError(f"Column {column} has {null_count} null values")

def check_unique(df: DataFrame, columns: list):
    for column in columns:
        unique_count = df.select(column).distinct().count()
        total_count = df.count()
        if unique_count != total_count:
            raise ValueError(f"Column {column} has duplicate values")