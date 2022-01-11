import sys
from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce, regexp_extract, year, lit
from pyspark.sql.utils import AnalysisException

from src.helper.constants import Constants as C
from src.processor.processor_base import ProcessorBase
from src.structs.recipe_schema import RecipeSchema

# Looking as current data as max we can have 2 partition.
NUM_REPARTITION = 2


class PreProcessData(ProcessorBase):
    """
    This class is responsible for
    1. reading data from given input path
    2. Applying schema to them
    3. Derive few columns for further process
    4. Persist as optimal storage
    """

    def __init__(self, spark: SparkSession, logger: Logger, input_path: str, output_path: str):
        super().__init__(spark, logger)
        self.spark = spark
        self.logger = logger
        self.input_path = input_path  # "input"
        self.output_path = output_path  # "pre_processed_data"

    def _extract(self):
        """
        Read Json data and apply schema with proper data type.
        :return: DataFrame
        """
        try:
            self.logger.info("Reading json data and applying schema : " + str(RecipeSchema.RECIPE))
            return self.spark.read.json(self.input_path, schema=RecipeSchema.RECIPE)
        except AnalysisException as e:
            self.logger.info("Failed to read file path : " + self.input_path)
            self.logger.exception("Error in extract method " + str(e))
            sys.exit(400)

    def _transform(self, data: DataFrame):
        """
        Transform function extract cooking time and prep time in seconds.
        Also extract year, so that we can store this processed data as a partition.
        :param data: DataFrame
        :return: DataFrame
        """
        try:
            self.logger.info("Extracting cooking time and prep time in seconds along with year.")
            return ProcessData.transform(data)
        except AnalysisException as e:
            self.logger.info("Column does not exists in : " + str(data.columns))
            self.logger.exception("Error operation " + str(e))
            sys.exit(400)

    def _load(self, data: DataFrame):
        """
        Partitioning data by year and storing as parquet.
        :param data: DataFrame
        :return: None
        """
        try:
            self.logger.info("Writing preprocessed data as parquet partitioned by each year.")
            data.repartition(NUM_REPARTITION).write.partitionBy(C.YEAR).mode("overwrite").parquet(self.output_path)
        except AnalysisException as e:
            self.logger.info("Column does not exists in : " + str(data.columns))
            self.logger.exception("Error operation " + str(e))
            sys.exit(400)


class ProcessData:
    def __init__(self, data: DataFrame):
        self.data = data

    @staticmethod
    def transform(data: DataFrame):
        """
        Transform function extract cooking time and prep time in seconds.
        Also extract year, so that we can store this processed data as a partition.
        :param data: DataFrame
        :return: DataFrame
        """
        return data.withColumn(
            C.COOKTIME_SEC,
            coalesce(regexp_extract(C.COOKTIME, r'(\d+)H', 1).cast('int'), lit(0)) * 3600 +
            coalesce(regexp_extract(C.COOKTIME, r'(\d+)M', 1).cast('int'), lit(0)) * 60 +
            coalesce(regexp_extract(C.COOKTIME, r'(\d+)S', 1).cast('int'), lit(0))
        ).withColumn(
            C.PREPTIME_SEC,
            coalesce(regexp_extract(C.PREPTIME, r'(\d+)H', 1).cast('int'), lit(0)) * 3600 +
            coalesce(regexp_extract(C.PREPTIME, r'(\d+)M', 1).cast('int'), lit(0)) * 60 +
            coalesce(regexp_extract(C.PREPTIME, r'(\d+)S', 1).cast('int'), lit(0))
        ).withColumn(C.YEAR, year(col(C.DATEPUBLISHED)))
