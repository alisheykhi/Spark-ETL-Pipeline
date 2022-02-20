package ir.brtech

import java.io.{File, FileNotFoundException, IOException}
import org.ini4j.Wini
import ETL.error

class ConfigHandler(initFilePath: String,
                    sparkSession: String,
                    Stream1ReadStream:String,
                    Stream2ReadStream:String,
                    Stream1Schema:String,
                    Stream2Schema:String,
                    writeStream:String) {


  val ini: Wini = {
    try {
      new Wini(new File(initFilePath))
    } catch {
      case x: FileNotFoundException =>
        println("Exception: File missing")
        error("Exception - ", x)
        null
      case x: IOException =>
        println("Input/output Exception")
        error("Exception - ", x)
        null
    }
  }

  val requiredSections = Map[String,String] (
    sparkSession -> "spark.app.name",
    sparkSession -> "spark.master",
    Stream1ReadStream -> "format",
    Stream1ReadStream -> "kafka.bootstrap.servers",
    Stream1ReadStream -> "subscribe",
    Stream1ReadStream -> "startingOffsets",
    Stream1ReadStream -> "maxOffsetsPerTrigger",
    Stream2ReadStream -> "format",
    Stream2ReadStream -> "kafka.bootstrap.servers",
    Stream2ReadStream -> "subscribe",
    Stream2ReadStream -> "startingOffsets",
    Stream2ReadStream -> "maxOffsetsPerTrigger",
    Stream1Schema -> "message.delimiter",
    Stream1Schema ->  "all.columns.name",
    Stream2Schema -> "message.delimiter",
    Stream2Schema ->  "all.columns.name",
    writeStream -> "output.mode",
    writeStream -> "trigger.processingTime",
    writeStream -> "option.checkpointLocation"
  ) foreach {
    case (section, key) =>
      require(try !ini.get(section).isEmpty
      catch {case x:NullPointerException => false }
        , s"The section $section is required.")
      require(try Util.getValueBySection(ini.get(section), key).nonEmpty
        catch  {case x:NullPointerException => false }
        , s"The $key key in $section section is required.")
  }


  val sparkSessionSection = ini.get(sparkSession)
  val sparkStream1ReadStream = ini.get(Stream1ReadStream)
  val sparkStream2ReadStream = ini.get(Stream2ReadStream)
  val sparkStream1Schema = ini.get(Stream1Schema)
  val sparkStream2Schema = ini.get(Stream2Schema)
  val writeSection = ini.get(writeStream)



}

object ConfigHandler{
  def apply(initFilePath: String,
            sparkSession: String,
            Stream1ReadStream: String,
            Stream2ReadStream: String,
            Stream1Schema: String,
            Stream2Schema: String,
            writeStream: String): ConfigHandler =
    new ConfigHandler(initFilePath, sparkSession, Stream1ReadStream, Stream2ReadStream, Stream1Schema, Stream2Schema, writeStream)
}
