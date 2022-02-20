package ir.brtech.transform.stream2
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object APN extends Transform {
  override def process: Column = {
    Util.nvl(col("APN_C10"),"-1").as("APN")
  }
}
