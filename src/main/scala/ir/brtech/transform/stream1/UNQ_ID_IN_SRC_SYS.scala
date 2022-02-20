package ir.brtech.transform.stream1
import ir.brtech.Util.{scientificNotation, sha1toIntString}
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{col, sha1}

object UNQ_ID_IN_SRC_SYS extends Transform {
  override def process: Column = {
    functions.concat(
      scientificNotation(sha1toIntString(sha1(col("FILE_NAME")))),
      col("CDR_ID"))
      .as("UNQ_ID_IN_SRC_SYS")
  }
}
