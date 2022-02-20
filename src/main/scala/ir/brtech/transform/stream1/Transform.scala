package ir.brtech.transform.stream1

import org.apache.spark.sql.Column

trait Transform {
  def process: Column
}
