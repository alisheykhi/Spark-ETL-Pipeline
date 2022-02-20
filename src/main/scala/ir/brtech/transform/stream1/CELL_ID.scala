package ir.brtech.transform.stream1
import ir.brtech.Util.{oraNumberHash, sha1toIntString}
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{col, concat, length, lit, lpad, sha1, when}

object CELL_ID extends Transform{
  override def process: Column = {
    sha1toIntString(oraNumberHash(
      when(col("SERVED_MCCMNC") === "43211",
        when(
          length(concat(
            col("SERVED_CI_SAC_ECI"),
            lpad(col("SERVED_CI_SAC_ECI"), 5, "0") ,
            when(length(
              col("SERVED_CI_SAC_ECI")) <= 5,
              lpad(col("SERVED_CI_SAC_ECI"), 5, "0"))
              .otherwise(col("SERVED_CI_SAC_ECI").cast("string"))
          )
          ) <= 15 , concat(col("SERVED_MCCMNC"),
            lpad(col("SERVED_LAC_TAI"), 5, "0"),
            lpad(col("SERVED_CI_SAC_ECI"), 5, "0")
          )
        ).otherwise (
          concat(
            col("SERVED_MCCMNC"),
            col("SERVED_LAC_TAI"),
            col("SERVED_CI_SAC_ECI").substr(lit(1), length(col("SERVED_CI_SAC_ECI"))-3),
            col("SERVED_CI_SAC_ECI").substr(-1, 1)
          )
        )
      ).otherwise("-1")
    ))
      .as("CELL_ID")

  }
}
