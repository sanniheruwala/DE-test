import argparse
from argparse import Namespace

from src.helper.log_builder import LogBuilder
from src.helper.spark_builder import SparkBuilder
from src.runner import Runner

logger = LogBuilder.get_logger()


# ----- ArgumentParser -----
def _parse_args():
    """
    Argument parser takes three arg(step, input path, output path).
    One has to choose following steps:
    1. preProcess
    2. transform
    :return: Namespace
    """
    parser = argparse.ArgumentParser(description='Optional app description')
    parser.add_argument("--step", required=True, choices=["preProcess", "transform"],
                        help="Please select at least one 1. preProcess 2.transform")
    parser.add_argument("--input", required=True, help="Please provide input data path")
    parser.add_argument("--output", required=True, help="Please provide output data path")

    return parser.parse_args()


# ----- Main function -----
def main():
    """
    Main function.
    :return: None
    """
    args: Namespace = _parse_args()
    spark = SparkBuilder.get_session(args.step, logger)
    Runner(spark, logger).execute(args.step, args.input, args.output)


if __name__ == "__main__":
    main()
