package ir.brtech.transform.stream2

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object RTD_AMT extends Transform {
  override def process: Column = {
    lit(null).cast("decimal(38,0)").as("RTD_AMT")
  }

}
