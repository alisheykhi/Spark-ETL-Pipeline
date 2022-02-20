package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object APPLICATION_PROTOCOL_ID extends Transform {
  override def process: Column = {
    lit("-1").cast("decimal(38,0)").as("APPLICATION_PROTOCOL_ID")
  }
}
