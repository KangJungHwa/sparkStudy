package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object StringReduce {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd=sc.parallelize(List("a","b","c","d"),2)
      val rdd2=rdd.reduce(_+_)
      println(rdd2)
  
      sc.stop
   }
}