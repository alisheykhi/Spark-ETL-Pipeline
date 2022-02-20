package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object CHRG_REG extends Transform {
  override def process: Column = {
  col("CHARGEREGION_C71").as("CHRG_REG")
  }
}
