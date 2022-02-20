package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat, lit, when}

object CELL_CODE extends Transform {
  override def process: Column = {

    when((col("LAC_TAI_C72").isNull).and(col("CI_SAC_ECI_C73").isNull),"")
      .otherwise(concat(col("MCC_MNC_C37"),lit("|"),
        col("LAC_TAI_C72"),lit("|"),col("CI_SAC_ECI_C73")))
      .cast("string")
      .as("CELL_CODE")
  }
}
