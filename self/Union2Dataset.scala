package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object Union2Dataset {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.textFile("c:\\dataset\\apache.txt").flatMap(x=>x.split(" ")).filter(x=>x.nonEmpty).map(x=>(x,1)).reduceByKey(_ + _)
      val rdd2=sc.textFile("c:\\dataset\\apache.txt").flatMap(x=>x.split(" ")).filter(x=>x.nonEmpty).map(x=>(x,1)).reduceByKey(_ + _)
      val rdd3=rdd1.union(rdd2).map(x=>x.swap).sortByKey(ascending=false)
      rdd3.collect.foreach(println)
      
      sc.stop
   }
}