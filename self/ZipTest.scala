package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.compress.GzipCodec

object ZipTest {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val codec = classOf[org.apache.hadoop.io.compress.GzipCodec]
      val rdd1= sc.parallelize(1 to 1000)
      rdd1.saveAsTextFile("c:\\dataset\\output",codec)
  
      sc.stop
   }
}