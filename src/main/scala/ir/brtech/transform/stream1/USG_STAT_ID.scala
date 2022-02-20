package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object USG_STAT_ID extends Transform {
  override def process: Column = {

    lit("-1").cast("decimal(38,0)").as("USG_STAT_ID")
  }
}
