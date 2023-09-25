from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from helper.job_helper.job_helpers import get_db_creds
from logger import LOGGER


class DMProcessor:
    def __init__(self) -> None:
        self.df = None
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName('Spark_app') \
            .getOrCreate()

    def __repr__(self) -> str:
        return f'DMProcessor(df={self.df}, spark={self.spark})'

    def extract_data(self, source_path) -> DataFrame:
        """Extract data from a source.

        Args:
        ----
            source (str): The source from which data will be extracted.
            kwargs (dict): kwargs

        Returns:
        -------
            DataFrame: DataFrame.
        """

        self.df = self.spark.read.parquet(source_path)
        LOGGER.info(f"Rows extracted {self.df.count()}, format - parquet")

        return self.df

    @staticmethod
    def dm_logic(df: DataFrame) -> DataFrame:
        """Replace null values in a PySpark DataFrame with appropriate default values.

        Args:
        ----
            df (DataFrame): Input PySpark DataFrame.

        Returns:
        -------
            DataFrame: DataFrame with null values replaced.
        """
        if df is not None:
            df = df.select([col(column).alias(column.upper()) for column in df.columns])

        return df

    def transform_data(self) -> DataFrame:
        """Transform the extracted data.

        Returns
        -------
            DataFrame: DataFrame.
        """
        if self.df is not None:
            self.df = self.df.transform(self.dm_logic)
            return self.df

    def load_data(self, schema=None, table=None) -> None:
        """Load the transformed data into db.

        Args:
        ----
            schema (str): schema.
            table: table

        Returns:
        -------
            None
        """

        url, properties = get_db_creds()
        # self.df.show()
        # self.df.printSchema()
        if self.df is not None:
            LOGGER.info(f"Loading data to {table}, partitioned by {schema}")
            self.df.write.jdbc(url, table, mode="overwrite", properties=properties)
