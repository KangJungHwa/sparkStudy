package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object Zip {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= SContext.getSparkContext()
      val rdd1=sc.parallelize(1 to 5, 2)
      val rdd2=sc.parallelize(List("a","b","c","d","e"), 2)
      val rdd3=rdd1.zip(rdd2)
      rdd3.collect.foreach(println)
      sc.stop
   }
}