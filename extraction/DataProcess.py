import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, monotonically_increasing_id
from pyspark.sql.types import IntegerType, StringType
from logger import LOGGER
from pyspark.sql import DataFrame


class DataProcessor:
    def __init__(self) -> None:
        self.df = None
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName('Spark_app') \
            .getOrCreate()

    def __repr__(self) -> str:
        return f'DataProcessor(df={self.df}, spark={self.spark})'

    def extract_data(self, source: str, **kwargs) -> DataFrame:
        """Extract data from a source.

        Args:
        ----
            source (str): The source from which data will be extracted.
            kwargs (dict): kwargs

        Returns:
        -------
            DataFrame: DataFrame.
        """
        if source == 'csv' and os.path.exists(kwargs['path']):
            self.df = self.spark.read.option("header", True).csv(path=kwargs['path'])
            LOGGER.info(f"Rows extracted {self.df.count()}, format - {source}")

        return self.df

    @staticmethod
    def replace_null_values(df: DataFrame) -> DataFrame:
        """Replace null values in a PySpark DataFrame with appropriate default values.

        Args:
        ----
            df (DataFrame): Input PySpark DataFrame.

        Returns:
        -------
            DataFrame: DataFrame with null values replaced.
        """
        for col_name, data_type in df.dtypes:
            if isinstance(df.schema[col_name].dataType, StringType):
                df = df.withColumn(col_name, when(col(col_name).isNull(), lit("UNKNWN"))
                                   .otherwise(col(col_name)))
            elif isinstance(df.schema[col_name].dataType, IntegerType):
                df = df.withColumn(col_name, when(col(col_name).isNull(), lit(-1))
                                   .otherwise(col(col_name)))
            else:
                pass

        return df

    @staticmethod
    def add_surrogate_key(df: DataFrame) -> DataFrame:
        """Add a surrogate key column to a PySpark DataFrame.

        Args:
        ----
            df (DataFrame): Input PySpark DataFrame.

        Returns:
        -------
            DataFrame: DataFrame with the added surrogate key column.
        """
        return df.withColumn("surrogate_key", monotonically_increasing_id())

    def transform_data(self) -> DataFrame:
        """Transform the extracted data.

        Returns
        -------
            DataFrame: DataFrame.
        """
        if self.df is not None:
            self.df = self.df.transform(self.replace_null_values) \
                .transform(self.add_surrogate_key)
            return self.df

    def spec_transform(self, fun) -> DataFrame:
        """Specific transformations the extracted data.

        Args:
        ----
            fun (function): function with specific logic
        Returns
        -------
            DataFrame: DataFrame.
        """
        if self.df is not None:
            self.df = fun(self.df)
            return self.df

    def load_data(self, destination: str, part_cols: list) -> None:
        """Load the transformed data into a destination.

        Args:
        ----
            destination (str): The destination where the data will be loaded.
            part_cols: columns for partitioning

        Returns:
        -------
            None
        """
        if self.df is not None:
            LOGGER.info(f"Loading data to {destination}, partitioned by {part_cols}")
            self.df.write.partitionBy(*part_cols).mode("overwrite").parquet(destination)
