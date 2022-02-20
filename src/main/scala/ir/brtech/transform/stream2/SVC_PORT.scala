package ir.brtech.transform.stream2
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object SVC_PORT extends Transform {
  override def process: Column = {
    Util.nvl(col("SERVICEPORT_C23"),"-1").cast("decimal(38,0)").as("SVC_PORT")
  }
}
