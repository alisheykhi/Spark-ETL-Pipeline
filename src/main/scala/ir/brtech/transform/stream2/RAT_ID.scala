package ir.brtech.transform.stream2
import ir.brtech.Util
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object RAT_ID extends Transform {

  override def process: Column = {
    //nvl(case when VGS_TEMP_1.RAT_TYPE_3GPP_RAT_C52 =0 then -1 else VGS_TEMP_1.RAT_TYPE_3GPP_RAT_C52 end,-1)
    Util.nvl(when(col("RAT_TYPE_3GPP_RAT_C52")==="0","-1")
      .otherwise(col("RAT_TYPE_3GPP_RAT_C52")),"-1" )
      .cast("decimal(38,0)")
      .as("RAT_ID")

  }
}
