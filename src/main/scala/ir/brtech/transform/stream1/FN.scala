package ir.brtech.transform.stream1
import ir.brtech.Util.{scientificNotation, sha1toIntString}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, sha1}

object FN extends Transform {
  override def process: Column =
    scientificNotation(sha1toIntString(sha1(col("FILE_NAME")))).as("FN")
}
