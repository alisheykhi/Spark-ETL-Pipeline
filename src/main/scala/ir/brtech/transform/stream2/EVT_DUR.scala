package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object EVT_DUR extends Transform {
  override def process: Column = {
    col("INTERVAL_C74").cast("decimal(38,0)").as("EVT_DUR")
  }
}
