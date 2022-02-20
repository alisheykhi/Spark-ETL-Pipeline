package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object USR_LOC_INFO extends Transform {
  override def process: Column = lit(null).as("USR_LOC_INFO").cast("String")
}
