package ir.brtech.transform.stream1
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

object RTD_AMT extends Transform {
  override def process: Column = {
    lit("0").as("RTD_AMT").cast("decimal(38,0)")
  }
}
