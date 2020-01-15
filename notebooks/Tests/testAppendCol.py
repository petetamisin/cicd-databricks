# Databricks notebook source
from dbxdemo.appendcol import with_status

source_data = [
  ("pete", "tamisin", "peter.tamisin@databricks.com"),
  ("jason", "baer", "jason.baer@databricks.com")
]
source_df = spark.createDataFrame(
    source_data,
    ["first_name", "last_name", "email"]
)

actual_df = with_status(source_df)

expected_data = [
    ("pete", "tamisin", "peter.tamisin@databricks.com", "checked"),
    ("jason", "baer", "jason.baer@databricks.com", "checked")
]
expected_df = spark.createDataFrame(
    expected_data,
    ["first_name", "last_name", "email", "status"]
)

assert(expected_df.collect() == actual_df.collect())


# COMMAND ----------



# COMMAND ----------


