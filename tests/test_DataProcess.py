import unittest
from pyspark.sql import SparkSession
from extraction.DataProcess import DataProcessor


class TestDataProcessor(unittest.TestCase):

    def setUp(self):
        # Initialize Spark session for testing
        self.spark = SparkSession.builder \
            .appName('SparkTestApp') \
            .getOrCreate()
        self.data_processor = DataProcessor()

    def tearDown(self):
        self.spark.stop()

    def test_extract_data_csv(self):
        csv_path = 'data/test_data.csv'
        test_df = self.spark.createDataFrame([("John", 'jo@mail.abc'), ("Alice", None)],
                                             ["name", "mail"])
        test_df.write.mode('overwrite').option("header", True).csv(csv_path)
        extracted_df = self.data_processor.extract_data('csv', path=csv_path)
        self.assertTrue(test_df.collect() == extracted_df.collect())

    def test_replace_null_values(self):
        test_df = self.spark.createDataFrame([("John", None), (None, 25)],
                                             ["name", "age"])
        replaced_df = self.data_processor.replace_null_values(test_df)
        replaced_df.show()

        # Check if null values are replaced correctly
        self.assertFalse(replaced_df.collect()[0]["name"] is None)
        self.assertFalse(replaced_df.collect()[1]["age"] is None)

    def test_add_surrogate_key(self):
        test_df = self.spark.createDataFrame([("John", 30), ("Alice", 25)],
                                             ["name", "age"])
        df_with_key = self.data_processor.add_surrogate_key(test_df)
        self.assertTrue("surrogate_key" in df_with_key.columns)

    def test_transform_data(self):
        test_df = self.spark.createDataFrame([("John", None), (None, 25)],
                                             ["name", "age"])
        self.data_processor.df = test_df
        transformed_df = self.data_processor.transform_data()

        # Check if null values are replaced and a surrogate key column is added
        self.assertFalse(transformed_df.collect()[0]["name"] is None)
        self.assertFalse(transformed_df.collect()[1]["age"] is None)
        self.assertTrue("surrogate_key" in transformed_df.columns)

    def test_load_data(self):
        test_df = self.spark.createDataFrame([("John", 30), ("Alice", 25)],
                                             ["name", "age"])
        self.data_processor.df = test_df
        destination = 'data/test_output.parquet'
        part_cols = ['name']
        self.data_processor.load_data(destination, part_cols)

        # Verify if the destination path exists
        self.assertTrue(self.data_processor.spark.read.parquet(destination).count() > 0)


if __name__ == '__main__':
    unittest.main()
