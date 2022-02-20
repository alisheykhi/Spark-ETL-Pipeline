package ir.brtech.transform.stream2

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object NUM_SESSS extends Transform {
  override def process: Column = {
    lit(null).as("NUM_SESSS").cast("decimal(38,0)")
  }
}
