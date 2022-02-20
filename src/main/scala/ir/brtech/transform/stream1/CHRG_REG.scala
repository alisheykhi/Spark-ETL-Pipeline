package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object CHRG_REG extends Transform {
  override def process: Column = lit(null).cast("String").as("CHRG_REG")
}
