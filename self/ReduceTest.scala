package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object ReduceTest {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(1 to 10)
      val rdd2=rdd1.reduce(_+_)
      println(rdd2)
      sc.stop
   }
}