package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, to_date}

object EFF_TO_DT extends Transform {
  override def process: Column = {
    lit("1/1/2400").as("EFF_TO_DT")
  }
}