package Spark.com.spark.sql
import org.apache.spark.{SparkConf, SparkContext}

class GetSparkContext {
  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("PassingFunctionSample")
    new SparkContext(conf)
  }
}