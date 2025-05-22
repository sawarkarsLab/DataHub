import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test_session")
        .master("local[2]")
        .getOrCreate()
    )
