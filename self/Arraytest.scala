package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object Arraytest {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val list = List("a","b","c","d")
      for(i <- 0 to list.length-1  ){
        println(list(i))
      }
  
      sc.stop
   }
}