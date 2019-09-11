package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object CountByValue {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd=sc.textFile("/tmp/apache.txt").flatMap(_.split(" ")).countByValue()
      val rdd2=sc.parallelize(rdd.toList).map(x=>x.swap).sortByKey(ascending=false)
            rdd2.collect.foreach(println)
      sc.stop
   }
}