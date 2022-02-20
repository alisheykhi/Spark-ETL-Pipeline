package ir.brtech.transform.stream2
import ir.brtech.Util.{scientificNotation, sha1toIntString}
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{col, sha1}


object INTRNET_ACCS_USG_ID extends Transform{
  override def process: Column = {
    sha1toIntString(
      sha1(
        functions.concat(
          scientificNotation(sha1toIntString(sha1(col("FILE_NAME")))),
          scientificNotation(col("SEQUENCE_C2").cast("String")),
          scientificNotation(col("MSISDN_C8").cast("String")),
          scientificNotation(col("SESSIONID_C36").cast("String")),
          scientificNotation(col("TIMESTAMP_C1").cast("String")),
          scientificNotation(col("RATINGGROUP_C66").cast("String"))
        )
      )
    ).as("INTRNET_ACCS_USG_ID")
  }
}
