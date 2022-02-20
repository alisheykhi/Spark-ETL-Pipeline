package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object URL extends Transform {
  override def process: Column = {
    col("DESTINATION_ADDRESS").cast("string").as("URL")
  }
}
