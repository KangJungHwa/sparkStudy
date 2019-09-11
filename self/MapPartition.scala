package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object MapPartition {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(1 to 10, 3)
      val rdd2=rdd1.mapPartitions(numbers=>
        {
         println("db  connection")
         numbers.map(number=>number +1)
      
      }) 
      rdd2.collect.foreach(println)
      sc.stop
   }
}