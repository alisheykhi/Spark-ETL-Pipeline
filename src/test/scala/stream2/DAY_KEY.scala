package stream2

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec
import test.TestWrapper
import ir.brtech.transform.stream2.{DAY_KEY=> ETL}

class DAY_KEY
  extends AnyFunSpec
    with DataFrameComparer
    with TestWrapper {

    it(s"Equality test for ${this.getClass.getSimpleName} in stream2") {
      val (transformedDf,expectedDf ) = getTransformedAndExpectedDf(ETL.process, col(this.getClass.getSimpleName))
      assertSmallDataFrameEquality(transformedDf, expectedDf)
    }
}