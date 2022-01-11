import sys
from logging import Logger

from pyspark.sql import SparkSession


class SparkBuilder:
    """
    Spark session provider class.
    We can add few more session such as hive, mllib, sql context providers.
    """

    @staticmethod
    def get_session(step: str, logger: Logger) -> SparkSession:
        """
        Provides spark session for the job.
        :rtype: SparkSession
        """
        try:
            return SparkSession \
                .builder \
                .appName("Recipe step : " + step) \
                .getOrCreate()
        except Exception as e:
            logger.info("Failed to Create Spark Session !!!")
            logger.exception("Error in get_session function " + str(e))
            sys.exit(400)
