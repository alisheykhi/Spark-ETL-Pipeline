package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object GROSS_AMT extends Transform {
  override def process: Column = {
    lit("0").as("GROSS_AMT").cast("decimal(38,0)")
  }
}
