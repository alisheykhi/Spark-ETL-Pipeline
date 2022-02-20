package ir.brtech.transform.stream2
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, substring}

object DAY_KEY extends Transform {
  override def process: Column = {

    Util.gregorianToJalaliUDF(col("TIMESTAMP_C1"))
      .as("DAY_KEY")
      .cast("String")
  }
}
