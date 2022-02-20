package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object URL extends Transform {
  override def process: Column = {
    col("SERVICEDOMAIN_C22").as("URL")
  }
}
