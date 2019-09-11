package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object Substract {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(List("a","b","c","d","e"))
      val rdd2=sc.parallelize(List("a","b","f","g","h"))
      val rdd3=rdd1.subtract(rdd2)
      rdd3.collect.foreach(println)
/*      val rdd4=rdd1.intersection(rdd2)
      rdd4.collect.foreach(println)*/
      sc.stop
   }
}