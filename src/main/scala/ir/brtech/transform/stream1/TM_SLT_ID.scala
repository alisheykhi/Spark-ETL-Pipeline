package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, substring}

object TM_SLT_ID extends Transform {
  override def process: Column = {
    Util.nvl(substring(col("START_TIMESTAMP"),9,2),"-1")
      .cast("decimal(38,0)")
      .as("TM_SLT_ID")
  }
}
