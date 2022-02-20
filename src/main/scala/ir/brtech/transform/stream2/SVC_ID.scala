package ir.brtech.transform.stream2
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object SVC_ID extends Transform {
  override def process: Column = {
    Util.nvl(col("SERVICEID_C26"),"-1").cast("String").as("SVC_ID")
  }
}
