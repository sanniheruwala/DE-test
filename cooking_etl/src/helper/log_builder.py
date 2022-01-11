import logging
import sys

FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class LogBuilder:
    """
    Logger utility provider.
    We can add few more settings such as file handler or specific formatting for errors.
    """

    @staticmethod
    def get_con_handler():
        con_handler = logging.StreamHandler(sys.stdout)
        con_handler.setFormatter(FORMATTER)
        return con_handler

    @staticmethod
    def get_logger(logger_name="main"):
        """
        Create heler class for logging.
        :return: Logger
        """
        logger = logging.getLogger(logger_name)

        # better to have detailed log
        logger.setLevel(logging.DEBUG)

        logger.addHandler(LogBuilder.get_con_handler())
        logger.propagate = False

        return logger
