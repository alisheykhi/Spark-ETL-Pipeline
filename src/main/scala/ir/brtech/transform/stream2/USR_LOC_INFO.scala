package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object USR_LOC_INFO extends Transform {
  override def process: Column = {
    col("USER_LOC_INFO_C46").as("USR_LOC_INFO")
  }
}
