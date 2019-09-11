package Spark.com.spark.sql
import org.apache.spark.{SparkConf, SparkContext}

object TypeSafetyTest {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      
  
      sc.stop
   }
}