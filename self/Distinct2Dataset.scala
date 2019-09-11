package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object Distinct2Dataset {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.textFile("c:\\dataset\\2.txt").flatMap(x=>x.split(" "))
      val rdd2=sc.textFile("c:\\dataset\\3.txt").flatMap(x=>x.split(" "))
      val rdd3=rdd1.union(rdd2).distinct()
      rdd3.collect.foreach(println)
      sc.stop
   }
}