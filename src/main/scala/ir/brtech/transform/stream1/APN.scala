package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object APN extends Transform {
  override def process: Column = {
    col("APN").cast("string").as("APN")
  }
}
