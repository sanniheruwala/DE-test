from pyspark.sql.types import DateType, StringType, StructField, StructType

from src.helper.constants import Constants as C


class RecipeSchema:
    """
    Schema class for all reader jobs.
    We can add few more schema to handle additional types of structs.
    """
    RECIPE = StructType(
        [
            StructField(C.NAME, StringType(), nullable=False),
            StructField(C.INGREDIENTS, StringType(), nullable=False),
            StructField(C.URL, StringType()),
            StructField(C.IMAGE, StringType()),
            StructField(C.COOKTIME, StringType()),
            StructField(C.RECIPEYIELD, StringType()),
            StructField(C.DATEPUBLISHED, DateType()),
            StructField(C.PREPTIME, StringType()),
            StructField(C.DESCRIPTION, StringType())
        ]
    )
