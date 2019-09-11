package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object SContext {
  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("PassingFunctionSample")
    new SparkContext(conf)
  }
}