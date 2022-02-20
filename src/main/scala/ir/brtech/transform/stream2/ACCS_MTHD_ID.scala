package ir.brtech.transform.stream2

import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object ACCS_MTHD_ID extends Transform {
  override def process: Column = {
    Util.nvl(col("MSISDN_C8"), -1).cast("decimal(38,0)").as("ACCS_MTHD_ID")
  }

}
