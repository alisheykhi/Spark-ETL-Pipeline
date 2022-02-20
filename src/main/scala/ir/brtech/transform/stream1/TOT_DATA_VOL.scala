package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object TOT_DATA_VOL extends Transform {
  override def process: Column = {
    col("ACTUAL_USAGE").as("TOT_DATA_VOL").cast("decimal(38,0)")
  }
}
