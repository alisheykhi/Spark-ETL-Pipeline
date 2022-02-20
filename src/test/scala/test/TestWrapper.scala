package test

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait TestWrapper {

  val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("ETLTest")
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.memory", "2g")
    .set("spark.executor.memory", "6g")
    .set("spark.executor.instances", "1")
    .set("spark.sql.shuffle.partitions", "1")



  val spark = SparkSession.builder().config(conf).getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def readCSV(path: String): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load(path)
  }

  lazy val stream1Input = readCSV("path_to_input_file")

  lazy val stream2Input = readCSV("path_to_input_file")

  lazy val stream1Expected = readCSV("path_to_expected_file")

  lazy val stream2Expected = readCSV("path_to_expected_file")

  def getTransformedAndExpectedDf (etlObjectName: Column,
             targetColumnName: Column,
             columnType:String = "String",
             streamName: String = "stream1")
  : (DataFrame,DataFrame) = {
    streamName match {
      case "stream1" => (stream2Input.select(etlObjectName, col("RN")),
        stream2Expected.select(targetColumnName.cast(columnType), col("RN")))
      case  "stream2" => (stream1Input.select(etlObjectName, col("RN")),
        stream1Expected.select(targetColumnName.cast(columnType), col("RN")))
    }
  }
}
