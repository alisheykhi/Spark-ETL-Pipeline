package ir.brtech.transform.stream1

import ir.brtech.Util.{nvl, scientificNotation, sha1toIntString}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat, sha1}

object INTRNET_ACCS_USG_ID extends Transform {
  override def process: Column = {

    sha1toIntString(
      sha1(
        concat(
          scientificNotation(sha1toIntString(sha1(col("FILE_NAME")))),
          scientificNotation(col("CDR_ID").cast("string")),
          scientificNotation(col("SERVED_MSISDN").cast("string")),
          scientificNotation(col("START_TIMESTAMP").cast("string")),
          scientificNotation(nvl(col("RATING_GROUP").cast("string"),"")),
          scientificNotation(col("RAT_TYPE").cast("string"))
        )
      )
    )
      .as("INTRNET_ACCS_USG_ID")
  }
}
