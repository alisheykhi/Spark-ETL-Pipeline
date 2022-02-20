package stream1



import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import ir.brtech.transform.stream1.{DAY_KEY => ETL}
import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec
import test.TestWrapper


class DAY_KEY
  extends AnyFunSpec
    with DataFrameComparer
    with TestWrapper {

  it(s"Equality test for ${this.getClass.getSimpleName} in stream1") {
    val (transformedDf,expectedDf )  = getTransformedAndExpectedDf(
      ETL.process,
      col(this.getClass.getSimpleName),
      "String",
      "stream1")
    assertSmallDataFrameEquality(transformedDf, expectedDf)
  }

}
