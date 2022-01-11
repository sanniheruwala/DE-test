import sys
from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, lower, when, avg
from pyspark.sql.utils import AnalysisException

from src.helper.constants import Constants as C
from src.processor.processor_base import ProcessorBase

# For CSV output as single file
NUM_REPARTITION = 1


class TransformBeefData(ProcessorBase):
    """
    This class is responsible for
    1. reading data from given input path
    2. Applying filter for beef
    3. Extract total cooking time
    4. Extracting difficult recipes
    5. Persist as optimal storage
    """

    def __init__(self, spark: SparkSession, logger: Logger, input_path: str, output_path: str):
        super().__init__(spark, logger)
        self.spark = spark
        self.logger = logger
        self.input_path = input_path
        self.output_path = output_path

    def _extract(self):
        """
        Reading preprocessed data.
        :return: DataFrame
        """
        try:
            self.logger.info("Reading preprocessed data from " + self.input_path)
            return self.spark.read.parquet(self.input_path)
        except AnalysisException as e:
            self.logger.info("Failed to read file path : " + self.input_path)
            self.logger.exception("Error in extract method " + str(e))
            sys.exit(400)

    def _transform(self, data: DataFrame):
        """
        Selecting records where ingredients are having beef.
        Extracting difficulties based on overall time it took to cook.
        Grouping them by difficulties and getting average cook time in mins.
        :param data: DataFrame
        :return: DataFrame
        """
        try:
            self.logger.info("Selecting ingredients having BEEF and calculating difficulties.")
            return ProcessBeef.transform(data)
        except AnalysisException as e:
            self.logger.info("Column does not exists in : " + str(data.columns))
            self.logger.exception("Error operation " + str(e))
            sys.exit(400)

    def _load(self, data: DataFrame):
        """
        Generating CSV as output file.
        :param data: DataFrame
        :return: DataFrame
        """
        try:
            self.logger.info("Writing data as csv and columns are : " + str(data.columns))
            data.repartition(NUM_REPARTITION).write.option("header", "true").mode(C.OVERWRITE).csv(self.output_path)
        except AnalysisException as e:
            self.logger.info("Column does not exists in : " + str(data.columns))
            self.logger.exception("Error operation " + str(e))
            sys.exit(400)


class ProcessBeef:
    def __init__(self, data: DataFrame):
        self.data = data

    @staticmethod
    def transform(data: DataFrame):
        """
        Selecting records where ingredients are having beef.
        Extracting difficulties based on overall time it took to cook.
        Grouping them by difficulties and getting average cook time in mins.
        :param data: DataFrame
        :return: DataFrame
        """
        return data.filter(lower(col(C.INGREDIENTS)).contains(C.BEEF)) \
            .withColumn(C.TOTAL_COOK_TIME, col(C.COOKTIME_SEC) + col(C.PREPTIME_SEC)) \
            .withColumn(C.DIFFICULTY,
                        when((col(C.TOTAL_COOK_TIME) < 1800), lit(C.EASY))
                        .otherwise(
                            when((col(C.TOTAL_COOK_TIME) >= 1800) & (col(C.TOTAL_COOK_TIME) <= 3600), lit(C.MEDIUM))
                                .otherwise(
                                when((col(C.TOTAL_COOK_TIME) > 3600), lit(C.HARD)))
                        )) \
            .groupBy(C.DIFFICULTY) \
            .agg((avg(col(C.TOTAL_COOK_TIME)) / 60).alias(C.AVG_TOTAL_COOK_TIME))
