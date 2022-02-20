package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object SRC_SYS_CRTN_DT extends Transform {
  override def process: Column = {
    lit(null).cast("TIMESTAMP").as("SRC_SYS_CRTN_DT")
  }
}
