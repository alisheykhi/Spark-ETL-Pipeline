package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{current_date, current_timestamp}

object UPD_DT extends Transform {
  override def process: Column = {
    current_timestamp().as("UPD_DT")
  }
}
