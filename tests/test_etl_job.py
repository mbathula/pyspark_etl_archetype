import unittest
from pyspark.sql import SparkSession
from etl.extract import extract_data
from etl.base_transformations import BaseTransform
from etl.us_transformations import USTransform
from etl.mx_transformations import MXTransform
from etl.ca_transformations import CATransform
from utils.data_quality import check_nulls, check_unique


class TestDataPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("PySparkETLTest") \
            .master("local[*]") \
            .getOrCreate()

        cls.sample_data = [
            (1, "Electronics", "Mobile", "Sales", 100),
            (2, "Clothing", "Men", "Sales", 200),
            (3, "Home", "Furniture", "Sales", 300)
        ]

        cls.schema = ["id", "category", "sub_category", "dept", "amount"]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_extract_data(self):
        df = self.spark.createDataFrame(self.sample_data, self.schema)
        df.write.mode('overwrite').parquet('/tmp/test_extract_data')

        extracted_df = extract_data(self.spark, '/tmp/test_extract_data')
        self.assertEqual(extracted_df.count(), 3)

    def test_base_transform(self):
        df = self.spark.createDataFrame(self.sample_data, self.schema)
        transformer = BaseTransform(df)
        transformed_df = transformer.transform_category() \
            .transform_sub_category() \
            .transform_dept() \
            .get_transformed_df()

        self.assertTrue("category" in transformed_df.columns)
        self.assertTrue("sub_category" in transformed_df.columns)
        self.assertTrue("dept" in transformed_df.columns)

    def test_us_transform(self):
        df = self.spark.createDataFrame(self.sample_data, self.schema)
        transformer = USTransform(df)
        transformed_df = transformer.transform_category() \
            .transform_sub_category() \
            .transform_dept() \
            .transform_currency() \
            .get_transformed_df()

        self.assertTrue("currency" in transformed_df.columns)
        self.assertEqual(transformed_df.filter(transformed_df.currency == 'USD').count(), 3)

    def test_mx_transform(self):
        df = self.spark.createDataFrame(self.sample_data, self.schema)
        transformer = MXTransform(df)
        transformed_df = transformer.transform_category() \
            .transform_sub_category() \
            .transform_dept() \
            .transform_currency() \
            .get_transformed_df()

        self.assertTrue("currency" in transformed_df.columns)
        self.assertEqual(transformed_df.filter(transformed_df.currency == 'MXN').count(), 3)

    def test_ca_transform(self):
        df = self.spark.createDataFrame(self.sample_data, self.schema)
        transformer = CATransform(df)
        transformed_df = transformer.transform_category() \
            .transform_sub_category() \
            .transform_dept() \
            .transform_currency() \
            .get_transformed_df()

        self.assertTrue("currency" in transformed_df.columns)
        self.assertEqual(transformed_df.filter(transformed_df.currency == 'CAD').count(), 3)

    def test_check_nulls(self):
        df = self.spark.createDataFrame(self.sample_data, self.schema)
        with self.assertRaises(ValueError):
            check_nulls(df, ['non_existent_column'])

    def test_check_unique(self):
        df = self.spark.createDataFrame(self.sample_data, self.schema)
        with self.assertRaises(ValueError):
            check_unique(df, ['category'])


if __name__ == '__main__':
    unittest.main()