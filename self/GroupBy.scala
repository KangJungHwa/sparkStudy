package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.mapValues

object GroupBy {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize( 1  to 10)
      val rdd2=rdd1.groupBy{
        case i: Int if(i % 2==0) => "even"
        case _ => "odd"
      }
      rdd2.collect.foreach(println)
      sc.stop
   }
}