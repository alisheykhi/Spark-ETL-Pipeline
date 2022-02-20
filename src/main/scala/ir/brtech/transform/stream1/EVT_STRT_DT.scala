package ir.brtech.transform.stream1

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_timestamp}

object EVT_STRT_DT extends Transform {
  override def process: Column =
    to_timestamp(col("START_TIMESTAMP"), "yyyyMMddHHmmss")
    .as("EVT_STRT_DT")
}
