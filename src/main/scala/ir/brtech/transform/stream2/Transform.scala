package ir.brtech.transform.stream2

import org.apache.spark.sql.Column

trait Transform {
  def process: Column
}
