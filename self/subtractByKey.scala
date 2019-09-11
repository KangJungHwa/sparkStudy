package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object subtractByKey {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(List("a","b","c","d","e")).map(x=>(x,1))
      val rdd2=sc.parallelize(List("a","b","f")).map(x=>(x,2))
      val rdd3=rdd1.subtractByKey(rdd2)
      rdd3.collect.foreach(println)
//      val rdd4=rdd1.union(rdd2)
//      rdd4.collect.foreach(print)
//      val rdd5=rdd1.cartesian(rdd2)
//      rdd5.collect.foreach(print)
      sc.stop
   }
}

