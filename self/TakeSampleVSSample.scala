package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object TakeSampleVSSample {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(1 to 10000)
      
      //첫번째 인자는 
      val rdd2=rdd1.sample(false,0.5,1)
      val rdd3=rdd1.sample(true,1.5,1)
      rdd1.takeSample(false, 1000)
      
      println(rdd2.collect.length)
      println(rdd3.collect.length)
      
      println(rdd2.collect.mkString(", "))
      println(rdd3.collect.mkString(", "))
      sc.stop
   }
}