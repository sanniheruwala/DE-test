import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.helper.constants import Constants as C
from src.processor.transform_beef_data import ProcessBeef


@pytest.mark.usefixtures("spark")
def test_process_transform(spark: SparkSession):
    """
    Transform processed data to add difficulties and
    get average cooking time per level.
    """

    current_path = Path(os.path.dirname(os.path.realpath(__file__)))
    input_path = str(current_path) + "/resources/input/pre_processed/"
    output_path = str(current_path) + "/resources/output/processed_beef_data.parquet"

    test_df = spark.read.parquet(input_path)
    new_df = ProcessBeef.transform(test_df)

    assert new_df.count() == 3, "Number records miss matched then expected."
    assert C.DIFFICULTY in new_df.columns, "difficulties column not found."
    assert C.AVG_TOTAL_COOK_TIME in new_df.columns, "avg_cook_time column not found."

    expected_df = spark.read.parquet(output_path)
    intersected_df = expected_df.intersect(new_df)
    expected_count = intersected_df.count()
    actual_count = new_df.count()

    assert actual_count == expected_count, "Transformation generated wrong records."
