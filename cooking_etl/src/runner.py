from logging import Logger

from pyspark.sql import SparkSession

from src.processor.pre_process_data import PreProcessData
from src.processor.transform_beef_data import TransformBeefData


class Runner:
    """
    This class is responsible for running the processes.
    Currently it support pre processing and transformation.

    Below process can be moved to config manager.
    """

    def __init__(self, spark: SparkSession, logger: Logger):
        self.spark = spark
        self.logger = logger

    def execute(self, step: str, input_path: str, output_path: str):
        """
        Responsible for running job based on input params.
        :param step: type of runner
        :param input_path: input data path
        :param output_path: output data path
        :return: None
        """
        self.logger.info("Running processor for " + step)
        if step == "preProcess":
            PreProcessData(self.spark, self.logger, input_path, output_path).run()
        elif step == "transform":
            TransformBeefData(self.spark, self.logger, input_path, output_path).run()
        else:
            raise ValueError("Step not found : " + step)
