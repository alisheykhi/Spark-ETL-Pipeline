package ir.brtech.transform.stream2
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object HNDST_ID extends Transform {
  override def process: Column = {

    when(col("IMEISV_C61") .rlike("[^0-9]") === true,-1)
      .otherwise(Util.nvl(col("IMEISV_C61"),-1)).cast("decimal(38,0)").as("HNDST_ID")
  }
}
