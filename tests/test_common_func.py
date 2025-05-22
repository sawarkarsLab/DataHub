import pytest
from pyspark.sql import Row
from modules import common_func

def test_normalize_column_names(spark):
    data = [Row(FirstName="John", LastName="Doe")]
    df = spark.createDataFrame(data)
    result = common_func.normalize_column_names(df)
    assert "first_name" in result.columns
    assert "last_name" in result.columns

def test_add_ingestion_metadata(spark):
    data = [Row(id=1, name="Alice")]
    df = spark.createDataFrame(data)
    df = common_func.add_ingestion_metadata(df, source="salesforce")
    
    assert "ingestion_source" in df.columns
    assert "ingestion_timestamp" in df.columns
    row = df.collect()[0]
    assert row["ingestion_source"] == "salesforce"

def test_write_parquet(tmp_path, spark):
    df = spark.createDataFrame([Row(a=1, b="x")])
    path = str(tmp_path / "parquet_test")
    
    common_func.write_parquet(df, path, mode="overwrite")
    
    result_df = spark.read.parquet(path)
    assert result_df.count() == 1
    assert "a" in result_df.columns

def test_write_iceberg_table(monkeypatch, spark):
    # Simulate Iceberg write without actual Hive catalog
    monkeypatch.setattr(common_func, "spark_write", lambda df, tbl, mode: df)
    df = spark.createDataFrame([Row(a=1)])
    try:
        common_func.write_iceberg_table(df, "demo", "table", mode="overwrite")
        success = True
    except Exception:
        success = False
    assert success
