package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object THRGHPUT_AVG extends Transform {
  override def process: Column = {
    lit(null).as("THRGHPUT_AVG").cast("decimal(38,0)")
  }
}
