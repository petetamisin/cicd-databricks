// Databricks notebook source
import org.apache.spark.sql.Encoders

case class NationSchema(NationKey: Int, Name: String, RegionKey: Int,
                      Comment: String)

var nationSchema = Encoders.product[NationSchema].schema

val nationsDf = sqlContext.read
         .format("csv")
         .option("header", "false") //reading the headers
         .schema(nationSchema)
         .option("delimiter", "|")
         .load("/databricks-datasets/tpch/data-001/nation/")

case class RegionSchema(RegionKey: Int, Name: String, Comment: String)

var regionSchema = Encoders.product[RegionSchema].schema

val regionsDf = sqlContext.read
         .format("csv")
         .option("header", "false") //reading the headers
         .schema(regionSchema)
         .option("delimiter", "|")
         .load("/databricks-datasets/tpch/data-001/region/")

val joinedDf = nationsDf.join(regionsDf, nationsDf("RegionKey") === regionsDf("RegionKey"), "inner")
                        .select(nationsDf("NationKey"), nationsDf("Name").as("NationName"), regionsDf("RegionKey"), regionsDf("Name").as("RegionName"))

joinedDf.createOrReplaceTempView("V_ALL_NATIONS")

// This is a test


// COMMAND ----------

// this is another test
joinedDf.write.mode(SaveMode.Overwrite).csv("/tmp/foo.csv")
