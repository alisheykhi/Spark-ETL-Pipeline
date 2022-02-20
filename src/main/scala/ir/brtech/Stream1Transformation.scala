package ir.brtech
import org.apache.spark.sql.DataFrame
import transform.stream1._


class Stream1Transformation(df: DataFrame)  {
  def applyTransformation:DataFrame = {
    df.select(
        //select your transformation class
        DAY_KEY.process,
      )

  }
}
object Stream1Transformation {
  def apply(df: DataFrame): DataFrame = new Stream1Transformation(df).applyTransformation
}
