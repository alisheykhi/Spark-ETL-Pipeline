package ir.brtech.transform.stream2
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object USG_STAT_ID extends Transform {
  override def process: Column = {

    Util.nvl(col("CDRSTATUS_C65"),"-1").cast("decimal(38,0)").as("USG_STAT_ID")
  }
}
