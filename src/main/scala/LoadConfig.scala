import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class LoadConfig(master: String = "local", appName: String) {
  val sparkConf = new SparkConf().setMaster(master).setAppName(appName)

  def getSparkSession: SparkSession = {
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  def getSparkContext: SparkContext = {
    SparkContext.getOrCreate(sparkConf)
  }
}
