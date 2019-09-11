package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object Catesian {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(1 to 10)
      val rdd2=sc.parallelize(1 to 5)
      val rdd3=rdd1.cartesian(rdd2)
      rdd3.collect.foreach(println)
      sc.stop
   }
}