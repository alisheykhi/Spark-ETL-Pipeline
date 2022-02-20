package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object APPLICATION_ID extends Transform{
  override def process: Column = {
    Util.nvl(lit("-1"),"-1").cast("decimal(38,0)")
      .as("APPLICATION_ID")
  }
}
