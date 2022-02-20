package test

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait TestWrapper {

  val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("InternetAccessUsageTest")
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

  lazy val pgwDf = readCSV("src/test/resources/PGW_NEW.CSV")

  lazy val vgsDf = readCSV("src/test/resources/VGS.CSV")

  lazy val pgwTargetDf = readCSV("src/test/resources/INTR_ACCS_USG_PGW.CSV")

  lazy val vgsTargetDf = readCSV("src/test/resources/INTR_ACCS_USG_VGS.CSV")

  def getTransformedAndExpectedDf (etlObjectName: Column,
             targetColumnName: Column,
             columnType:String = "String",
             streamName: String = "vgs")
  : (DataFrame,DataFrame) = {
    streamName match {
      case "vgs" => (vgsDf.select(etlObjectName, col("RN")),
        vgsTargetDf.select(targetColumnName.cast(columnType), col("RN")))
      case  "pgw" => (pgwDf.select(etlObjectName, col("RN")),
        pgwTargetDf.select(targetColumnName.cast(columnType), col("RN")))
    }
  }
}
