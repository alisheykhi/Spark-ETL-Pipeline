package ir.brtech

import org.apache.spark.sql.DataFrame
import org.ini4j.Profile



class ApplySchema(sparkSchemaSection: Profile.Section, streamData: DataFrame) {

  val config = Util.getValueBySection(sparkSchemaSection,_:String)

  def getDFWithSchema: DataFrame = {

    val selectExprString = config("all.columns.name")
      .split(",")
      .zipWithIndex
      .map {
        case (field, index) =>
          "CAST(SplitParts[" + index + "] AS STRING) as " + field
      }
      .toSeq

    val streamDataWithSchema = streamData
      .selectExpr("CAST(value AS STRING) as CDR")
      .selectExpr(
        "split(CDR,'" + config("message.delimiter") + "') as SplitParts"
      )
      .selectExpr(selectExprString: _*)

    streamDataWithSchema
  }
}

object ApplySchema{
  def apply(sparkSchemaSection: Profile.Section, streamData: DataFrame): DataFrame =
      new ApplySchema(sparkSchemaSection, streamData).getDFWithSchema
}
