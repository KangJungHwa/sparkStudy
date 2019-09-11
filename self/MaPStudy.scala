package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object MaPStudy {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.textFile("c:\\dataset\\apache.txt")
      val rdd2=rdd1.flatMap(x =>x.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).map(x =>x.swap).sortByKey(ascending=false).map(x=>x.swap)
      rdd2.collect.foreach(println)
      sc.stop
   }
}