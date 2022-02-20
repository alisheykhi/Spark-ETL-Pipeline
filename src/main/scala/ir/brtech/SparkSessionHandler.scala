package ir.brtech

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.ini4j.Profile

import scala.collection.JavaConversions._

class SparkSessionHandler(sparkSessionSection: Profile.Section )  {

  val config = Util.getValueBySection(sparkSessionSection,_:String)

  def getSparkSession: SparkSession = {

    val conf: SparkConf = new SparkConf()
    for (optionKey <- sparkSessionSection.keySet()) {
      conf.set(optionKey, config(optionKey))
    }
    val sparkSession: SparkSession = config("spark.master") match {
      case "local" => SparkSession.builder().config(conf).getOrCreate()
      case _ => SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    }
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

}

object  SparkSessionHandler{
  def apply(sparkSessionSection: Profile.Section): SparkSession =
    new SparkSessionHandler(sparkSessionSection).getSparkSession
}
