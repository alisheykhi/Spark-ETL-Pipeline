package ir.brtech.transform.stream1

import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object MAIN_PROD_OFRING_ID extends Transform {
  override def process: Column = {
    lit("-1")
      .as("MAIN_PROD_OFRING_ID")
      .cast("decimal(38,0)")
  }
}