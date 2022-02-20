package ir.brtech.transform.stream1

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, expr, to_timestamp, unix_timestamp}

object EVT_END_DT extends Transform {
  override def process: Column =
    (unix_timestamp(
      to_timestamp(col("START_TIMESTAMP"), "yyyyMMddHHmmss"))
      + col("EXEC_DURATION") ).cast("timestamp")
      .as("EVT_END_DT")
}





