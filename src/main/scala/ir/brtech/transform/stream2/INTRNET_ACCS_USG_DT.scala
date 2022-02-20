package ir.brtech.transform.stream2
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.Column

object INTRNET_ACCS_USG_DT extends Transform {
  override def process: Column = {

    to_timestamp(col("TIMESTAMP_C1"),"yyyyMMddHHmmss").as("INTRNET_ACCS_USG_DT")
     }
}
