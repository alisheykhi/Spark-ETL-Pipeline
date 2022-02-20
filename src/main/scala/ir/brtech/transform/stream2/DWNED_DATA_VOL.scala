package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object DWNED_DATA_VOL extends Transform {
  override def process: Column = {
    col("DOWNLINKTRAFFICVOLUME_C34").cast("decimal(38,0)").as("DWNED_DATA_VOL")
  }
}
