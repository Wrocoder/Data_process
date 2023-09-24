from abc import ABC, abstractmethod
from pyspark.sql import SparkSession


class BaseJob(ABC):
    def __init__(self):
        # Initialize Spark session
        self.spark = SparkSession.builder.appName("ETLJob").getOrCreate()

    @abstractmethod
    def extract(self, params):
        """
        Extract data from source(s).
        This method should be implemented by subclasses.
        """
        pass

    @abstractmethod
    def transform(self):
        """
        Transform the extracted data.
        This method should be implemented by subclasses.
        """
        pass

    @abstractmethod
    def load(self, path, file_name):
        """
        Load the transformed data into a destination.
        This method should be implemented by subclasses.
        """
        pass

    def run(self):
        """
        Run the ETL job.
        This method orchestrates the ETL process by calling extract, transform, and load methods.
        """
        self.extract()
        self.transform()
        self.load()

