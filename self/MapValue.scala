
package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.mapValues

object MapValue {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(List("a","b","c")).map((_,1))
      val rdd2=rdd1.mapValues(x=> x*x)
      sc.stop
   }
}