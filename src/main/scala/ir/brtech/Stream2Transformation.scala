package ir.brtech
import org.apache.spark.sql.DataFrame
import transform.stream2._


class Stream2Transformation(df: DataFrame)  {
  def applyTransformation:DataFrame = {
    df.select(
      //select your transformation class
      DAY_KEY.process,
    )
  }
}
object Stream2Transformation {
  def apply(df: DataFrame): DataFrame = new Stream2Transformation(df).applyTransformation
}
