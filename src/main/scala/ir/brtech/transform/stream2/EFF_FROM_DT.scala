package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, to_date}

object EFF_FROM_DT extends Transform {
  override def process: Column = {
    lit("1/1/1900").as("EFF_FROM_DT")
  }
}
