package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object SESS_ID extends Transform {
  override def process: Column = {
    lit("-1").as("SESS_ID")
  }
}
