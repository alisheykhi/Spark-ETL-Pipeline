package ir.brtech

import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ini4j.Profile

import scala.collection.JavaConversions._
import scala.collection.mutable

class InputStreamHandler(
    inputStreamSection: Profile.Section,
    sparkSession: SparkSession
) {
  val config = Util.getValueBySection(inputStreamSection,_:String)
  val readStream: DataStreamReader = sparkSession.readStream.format(config("format"))
  val sparkReadStreamKeys: mutable.Set[String] =
    inputStreamSection.keySet().filter { x => x != "format" }

  def getInputStream: DataFrame = {
    for (optionKey <- sparkReadStreamKeys) {
      readStream.option(
        optionKey,
        config(optionKey)
      )
    }
    readStream.load()
    //    streamData
  }
}

object InputStreamHandler{
  def apply(inputStreamSection: Profile.Section, sparkSession: SparkSession): DataFrame =
      new InputStreamHandler(inputStreamSection, sparkSession).getInputStream
}
