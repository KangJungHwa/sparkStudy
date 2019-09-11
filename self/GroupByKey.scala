package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd=sc.parallelize(List("a","a","b","b","c","c","d")).map((_,1))
      val rdd1=rdd.groupByKey
      rdd1.collect.foreach(println)
      sc.stop
   }
}