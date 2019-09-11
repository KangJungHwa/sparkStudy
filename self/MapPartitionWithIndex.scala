package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionWithIndex {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1 = sc.parallelize(1 to 10, 3)
      val rdd2=rdd1.mapPartitionsWithIndex((idx,numbers) =>{
        numbers.map{
          case number if idx ==1 => number+1
          case number if idx ==2 || idx ==0 => number+10
        }
      }
      )
      rdd2.collect.foreach(println)      
      sc.stop
   }
}