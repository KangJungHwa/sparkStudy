package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object FilterString {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1 = sc.textFile("c:\\dataset\\apache.txt").filter(f=> f.nonEmpty)
      val rdd2 = rdd1.zipWithIndex().map(x=>x.swap)
      rdd2.collect.foreach{
        case(k,v) => println(s"${k}, ${v}")
      }
      
      
      sc.stop
   }
}