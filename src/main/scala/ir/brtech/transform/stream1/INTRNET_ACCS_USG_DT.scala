package ir.brtech.transform.stream1

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, from_utc_timestamp, to_timestamp}

object INTRNET_ACCS_USG_DT extends Transform {
  override def process: Column =
    to_timestamp(col("START_TIMESTAMP"), "yyyyMMddHHmmss")
      .as("INTRNET_ACCS_USG_DT")
}
