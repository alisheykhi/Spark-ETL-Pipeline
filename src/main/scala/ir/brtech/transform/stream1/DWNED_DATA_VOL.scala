package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object DWNED_DATA_VOL extends Transform {
  override def process: Column = {
    Util.nvl(col("DOWNLINK_VOLUME"),"-1").cast("decimal(38,0)").as("DWNED_DATA_VOL")
  }
}
