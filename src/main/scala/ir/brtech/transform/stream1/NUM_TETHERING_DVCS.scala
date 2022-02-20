package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object NUM_TETHERING_DVCS extends Transform {
  override def process: Column = {
    lit("-1").as("NUM_TETHERING_DVCS")
  }
}
