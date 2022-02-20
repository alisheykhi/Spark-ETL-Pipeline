package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object THRTTL_TYP_ID extends Transform {
  override def process: Column = {
    lit("-1").cast("decimal(38,0)").as("THRTTL_TYP_ID")
  }
}
