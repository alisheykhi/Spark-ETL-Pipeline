package ir.brtech.transform.stream1

import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object SVC_PORT extends Transform {
  override def process: Column = {
    col("SERVICE_PORT").cast("decimal(38,0)").as("SVC_PORT")
  }
}
