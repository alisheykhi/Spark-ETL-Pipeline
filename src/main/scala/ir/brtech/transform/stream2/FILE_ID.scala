package ir.brtech.transform.stream2

import ir.brtech.Util.sha1toIntString
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, sha1}

object FILE_ID extends Transform {

  override def process: Column =
    sha1toIntString(sha1(col("FILE_NAME"))).as("FILE_ID")
}
