package ir.brtech

import org.apache.spark.sql.{DataFrame, SparkSession}

object ETL extends App with AppLogger {
  val log4Path: String = args(0)
  val iniFilePath: String = args(1)

  LogHandler(log4Path)

  val config: ConfigHandler = ConfigHandler(iniFilePath,
                                                  "SparkSession",
                                                  "Stream1ReadStream",
                                                  "Stream2ReadStream",
                                                  "Stream1Schema",
                                                  "Stream2Schema",
                                                  "WriteStream")
  val sparkSession: SparkSession = SparkSessionHandler(config.sparkSessionSection)
  val pgwInputStream: DataFrame = InputStreamHandler(config.sparkStream1ReadStream, sparkSession)
  val Stream2InputStream: DataFrame = InputStreamHandler(config.sparkStream2ReadStream, sparkSession)

  val pgwInputStreamWithSchema: DataFrame = ApplySchema(config.sparkStream1Schema, pgwInputStream)
  val Stream2InputStreamWithSchema: DataFrame = ApplySchema(config.sparkStream2Schema, Stream2InputStream)

  val pgwQuery =  WriteStreamHandler(pgwInputStreamWithSchema,config.writeSection, sparkSession,"Stream1")
  val Stream2Query =  WriteStreamHandler(Stream2InputStreamWithSchema,config.writeSection, sparkSession,"Stream2")

  sparkSession.streams.awaitAnyTermination()
  pgwQuery.stop()
  Stream2Query.stop()
}
