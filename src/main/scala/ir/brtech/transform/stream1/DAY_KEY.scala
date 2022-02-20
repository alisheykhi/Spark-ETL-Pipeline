package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_timestamp}

object DAY_KEY extends Transform {
  override def process: Column =
    Util.gregorianToJalaliUDF(col("START_TIMESTAMP"))
          .as("DAY_KEY")
      .cast("String")
}
