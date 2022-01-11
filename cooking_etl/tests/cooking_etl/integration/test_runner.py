import pytest
from pyspark.sql import SparkSession

from src.helper.log_builder import LogBuilder
from src.runner import Runner

logger = LogBuilder.get_logger()


@pytest.mark.usefixtures("spark")
def test_wrong_step(spark: SparkSession):
    """
    Check behaviour of runner in case of any unmatched
    value provided.
    """
    STEP = "not_exist"
    INPUT = ""
    OUTPUT = ""
    with pytest.raises(ValueError, match=r'Step not found') as excInfo:
        Runner(spark, logger).execute(STEP, INPUT, OUTPUT)
    assert "Step not found" in str(excInfo.value)
