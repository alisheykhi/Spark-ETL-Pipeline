package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object UPLDED_DATA_VOL extends Transform {
  override def process: Column = {
    col("UPLINKTRAFFICVOLUME_C33").cast("decimal(38,0)").as("UPLDED_DATA_VOL")
  }
}
