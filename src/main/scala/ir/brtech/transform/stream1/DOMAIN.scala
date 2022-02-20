package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object DOMAIN extends Transform {
  override def process: Column = {

    when(col("DESTINATION_ADDRESS").rlike("^(([0-9]{1}|[0-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\\.){3}([0-9]{1}|[0-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$")=== false,col("DESTINATION_ADDRESS"))
      .otherwise("-1").as("DOMAIN")
  }
}
