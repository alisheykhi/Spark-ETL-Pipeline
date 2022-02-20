package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object SRC_SYS_ID extends Transform {
  override def process: Column = {
    lit("1117100").cast("decimal(38,0)").as("SRC_SYS_ID")
  }
}
