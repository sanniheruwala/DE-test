import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.helper.constants import Constants as C
from src.processor.pre_process_data import ProcessData
from src.structs.recipe_schema import RecipeSchema


@pytest.mark.usefixtures("spark")
def test_process_transform(spark: SparkSession):
    """
    Check whether preprocess able to transform data and create
    cooktime_sec, preptime_sec and year columns.
    """
    current_path = Path(os.path.dirname(os.path.realpath(__file__)))
    input_path = str(current_path) + "/resources/input/recipes-000.json"

    test_df = spark.read.json(input_path, schema=RecipeSchema.RECIPE)
    new_df = ProcessData.transform(test_df)

    assert new_df.count() == 484, "Number records miss matched then expected."
    assert C.COOKTIME_SEC in new_df.columns, "cooktime_sec column not found."
    assert C.PREPTIME_SEC in new_df.columns, "preptime_sec column not found."
