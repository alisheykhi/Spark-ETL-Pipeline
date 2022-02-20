package ir.brtech.transform.stream2

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_timestamp}

object EVT_END_DT extends Transform {
  override def process: Column = {
    to_timestamp(col("STOPTIME_C28"), "yyyyMMddHHmmssSSS")
      .as("EVT_END_DT")
  }
}
