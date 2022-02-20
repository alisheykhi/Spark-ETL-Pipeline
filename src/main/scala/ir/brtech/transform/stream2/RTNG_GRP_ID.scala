package ir.brtech.transform.stream2
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object RTNG_GRP_ID extends Transform {
  override def process: Column = {
    Util.nvl(col("RATINGGROUP_C66"),"-1")
      .cast("decimal(38,0)")
      .as("RTNG_GRP_ID")
  }
}
