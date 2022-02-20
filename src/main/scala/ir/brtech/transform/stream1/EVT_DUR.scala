package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object EVT_DUR extends Transform {
  override def process: Column = {
    col("EXEC_DURATION").cast("decimal(38,0)").as("EVT_DUR")
  }
}
