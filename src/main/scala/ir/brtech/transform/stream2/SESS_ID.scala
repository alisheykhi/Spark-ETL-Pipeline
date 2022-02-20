package ir.brtech.transform.stream2

import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object SESS_ID extends Transform {
  override def process: Column = {
    Util.nvl(col("SESSIONID_C36"),"-1").cast("String").as("SESS_ID")
  }
}
