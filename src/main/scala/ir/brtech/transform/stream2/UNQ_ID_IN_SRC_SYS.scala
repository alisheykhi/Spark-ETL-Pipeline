package ir.brtech.transform.stream2
import ir.brtech.Util.{scientificNotation, sha1toIntString}
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{col, concat, sha1}


object UNQ_ID_IN_SRC_SYS extends Transform {
  override def process: Column = {
    concat(
      scientificNotation(sha1toIntString(sha1(col("FILE_NAME")))),
      col("SEQUENCE_C2"))
      .as("UNQ_ID_IN_SRC_SYS")
  }
}
