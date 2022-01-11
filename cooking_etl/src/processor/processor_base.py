from abc import ABC, abstractmethod
from logging import Logger

from pyspark.sql import DataFrame, SparkSession


class ProcessorBase(ABC):
    """
    Base class provider for ETL process to downstream classes.
    """

    def __init__(self, spark: SparkSession, logger: Logger):
        self.spark = spark
        self.logger = logger

    @abstractmethod
    def _extract(self):
        """
        Reader method.
        """
        raise NotImplementedError

    @abstractmethod
    def _transform(self, data: DataFrame):
        """
        Base transform method.
        """
        raise NotImplementedError

    @abstractmethod
    def _load(self, df: DataFrame):
        """
        Writer method.
        """
        raise NotImplementedError

    def run(self):
        data = self._extract()
        transform_data = self._transform(data)
        self._load(transform_data)
