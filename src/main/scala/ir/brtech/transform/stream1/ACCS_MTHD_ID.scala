package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object ACCS_MTHD_ID extends Transform {
  override def process: Column = {
    Util.nvl(col("SERVED_MSISDN"), -1).cast("decimal(38,0)").as("ACCS_MTHD_ID")
  }
}

