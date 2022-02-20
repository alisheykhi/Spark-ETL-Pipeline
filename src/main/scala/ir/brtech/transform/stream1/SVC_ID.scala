package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object SVC_ID extends Transform {
  override def process: Column = {
    Util.nvl(col("SERVICE_IDENTIFIER"),"-1").cast("String").as("SVC_ID")
  }
}
