package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object MapWithPairRdd {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(List("a","b","c","d","e")).map(x => (x,1))
      val rdd2=sc.parallelize(List("g","f","c","d","e")).map(x => (x,2))
      val rdd3=rdd1.union(rdd2)
      rdd3.collect.foreach(println)
      val rdd4=rdd3.map{
        case(k,v) => (k, v+10)
      }
//      rdd4.collect.foreach(println)
//      val rdd5=rdd3.flatMap{
//        case(k,v) => (k, v+10)
//      }
      rdd4.collect.foreach(println)
      sc.stop
   }
}