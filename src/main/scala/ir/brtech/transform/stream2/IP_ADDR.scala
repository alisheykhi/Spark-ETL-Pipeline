package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object IP_ADDR extends Transform {
  override def process: Column = {
    when(col("SERVICEDOMAIN_C22").rlike("^(([0-9]{1}|[0-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\\.){3}([0-9]{1}|[0-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])$") === true,col("SERVICEDOMAIN_C22"))
      .otherwise(null).as("IP_ADDR")
  }
}
