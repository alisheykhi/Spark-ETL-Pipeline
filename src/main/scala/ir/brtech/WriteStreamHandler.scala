package ir.brtech

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ini4j.Profile



class WriteStreamHandler(
                          streamDF: DataFrame,
                          writeSection: Profile.Section,
                          sparkSession: SparkSession,
                          queryName: String
                        ) {
  val config = Util.getValueBySection(writeSection, _: String)

  def getWriteStream: StreamingQuery = {
    streamDF.writeStream
      .option(
        "checkpointLocation",
        config("option.checkpointLocation") + queryName
      )
      .trigger(
        Trigger.ProcessingTime(
          config("trigger.processingTime")
        )
      )
      .outputMode(
        config("output.mode")
      )
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.head(1).isEmpty) {

//          batchDF.show(false)


          val finalDf:DataFrame = queryName match {
            case "pgw_new" => Stream1Transformation(batchDF)
            case "vgs" => Stream2Transformation(batchDF)
          }


//            finalDf
//            .write
//            .mode(config("output.mode"))
//            .option("header", "true")
//            .format(config("file.out.format"))
//            .save(config("file.out.path") + queryName)

            finalDf
              .select(config("target.table.columns").split(",").map(col): _*)
              .write
              .insertInto(config("target.table"))


        } else {
          println(s"${queryName} batchDF head isEmpty: " + batchDF.head(1).isEmpty)
        }
      }
      .queryName(queryName)
      .start()

  }
}

object WriteStreamHandler {
  def apply(streamDF: DataFrame,
            writeSection: Profile.Section,
            sparkSession: SparkSession,
            queryName: String): StreamingQuery =
    new WriteStreamHandler(streamDF, writeSection, sparkSession, queryName).getWriteStream
}
