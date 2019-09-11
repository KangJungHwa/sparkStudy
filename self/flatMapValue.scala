package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object flatMapValue {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd=sc.parallelize(List((1,"a,b"), (2,"a,b"), (3,"a,b")))
      val rdd1=rdd.flatMapValues(v=>v.split(","))
      rdd1.collect.foreach(println)
      sc.stop
   }
}