package ir.brtech.transform.stream2
import ir.brtech.Util
import ir.brtech.Util.{oraNumberHash, oraSingleNumberRepresentationWithToNumber, scientificNotation, sha1toIntString}
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{col, conv, floor, length, lit, lpad, pmod, sha1, substring, when}

object CELL_ID extends Transform {
  override def process: Column = {


        when(col("LAC_TAI_C72").isNull.and(col("CI_SAC_ECI_C73").isNull),-1)
          .otherwise(
              Util.nvl(
                    when(col("MCC_MNC_C37") === "43211",
            when(
              length(
                functions.concat(
                lpad(conv(col("LAC_TAI_C72"), 16, 10),5,"0"),
                  when(length(conv(col("CI_SAC_ECI_C73"),16,10))<=5,
                    lpad(conv(col("CI_SAC_ECI_C73"),16,10),5,"0"))
                  .otherwise(conv(col("CI_SAC_ECI_C73"), 16,10))
                 )
              )
             <=10,
                sha1toIntString(oraNumberHash(
                functions.concat(
                  scientificNotation(lit("43211")),
                  scientificNotation(lpad(conv(col("LAC_TAI_C72"),16,10),5,"0")),
                  scientificNotation(lpad(conv(col("CI_SAC_ECI_C73"),16,10),5,"0"))
            )))).otherwise(
              sha1toIntString(oraNumberHash(
                functions.concat_ws(
                  "",
                  lit("43211"),
                  conv(col("LAC_TAI_C72"),16,10),
                  floor(conv(col("CI_SAC_ECI_C73"),16,10)/256),
                  pmod(conv(col("CI_SAC_ECI_C73"),16,10),lit(256)).cast("int")
                )
              )
          ))).otherwise(-1),-1))
   .as("CELL_ID")
  }

}
