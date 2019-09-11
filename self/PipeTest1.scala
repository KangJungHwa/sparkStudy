package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object PipeTest1 {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd=sc.parallelize(List("1,2,3","4,5,6","7,8,9"))
      val result=rdd.pipe("cut, -f, 1,3 -d ,")
      println(result.collect.mkString(", "))
      sc.stop
   }
}