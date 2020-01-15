// Databricks notebook source
// MAGIC %run "../ETL/LoadAllNationsDemo"

// COMMAND ----------

printf("Total Nations: %,d%n", joinedDf.count)

val expectedCount = 25
assert (joinedDf.count == expectedCount, s"Expected ${expectedCount} nations but found ${joinedDf.count}")


// COMMAND ----------


