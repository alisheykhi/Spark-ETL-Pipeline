package ir.brtech.transform.stream1
import ir.brtech.Util.nvl
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, when}

object RECORD_CLOSING_RSN_ID extends Transform {
  override def process: Column = {
    when (col("CAUSE_FOR_CLOSING").rlike("[^0-9]"), -1)
      .otherwise(nvl(col("CAUSE_FOR_CLOSING"), "-1"))
    .cast("decimal(38,0)").as("RECORD_CLOSING_RSN_ID")
  }
}
