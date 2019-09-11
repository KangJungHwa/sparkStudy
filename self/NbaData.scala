package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object NbaData {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      
  
      sc.stop
   }
}