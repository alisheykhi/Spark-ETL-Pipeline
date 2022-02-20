package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object HNDST_ID extends Transform {
  override def process: Column = {
    when(col("SERVED_IMEISV").rlike("[^0-9]") === true, -1)
      .otherwise(Util.nvl(col("SERVED_IMEISV"), "-1")).cast("decimal(38,0)").as("HNDST_ID")
  }
}
