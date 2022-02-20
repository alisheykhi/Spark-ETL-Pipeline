package ir.brtech.transform.stream2
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, floor, length, lit, lpad, pmod, sha1, substring, when}
object IR_DOMAIN_FLG extends Transform {
  override def process: Column = {

    when (substring(col("SERVICEDOMAIN_C22"),-3,3) === lit(".ir"),"1").otherwise("0")
      .cast("decimal(38,0)").as("IR_DOMAIN_FLG")
  }
}
