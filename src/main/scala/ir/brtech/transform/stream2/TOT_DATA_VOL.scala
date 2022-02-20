package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object TOT_DATA_VOL extends Transform {
  override def process: Column = {
    col("CHARGEUNIT_C67").cast("decimal(38,0)").as("TOT_DATA_VOL")
  }
}
