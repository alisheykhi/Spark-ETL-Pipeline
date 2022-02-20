package ir.brtech.transform.stream1
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object RAT_ID extends Transform {
  override def process: Column = {
  when(col("RAT_TYPE") === "20", "2")
      .when(col("RAT_TYPE") === "30", "1")
      .when(col("RAT_TYPE") === "40", "6")
      .otherwise("-1")
      .cast("decimal(38,0)")
      .as("RAT_ID")
  }
}
