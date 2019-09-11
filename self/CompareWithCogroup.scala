package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object CompareWithCogroup {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.textFile("c:\\dataset\\apache.txt").flatMap(x=>x.split(" ")).filter(x=>x.nonEmpty).map(x=>(x,1)).reduceByKey(_ + _).map(x=>x.swap)
      val rdd2=sc.textFile("c:\\dataset\\apache.txt").flatMap(x=>x.split(" ")).filter(x=>x.nonEmpty).map(x=>(x,1)).reduceByKey(_ + _).map(x=>x.swap)
      val rdd3=rdd1.cogroup(rdd2).sortByKey(ascending=false)
      rdd3.collect.foreach {
        case(k,(v1,v2)) =>{
          println(s"${k},[${v1.mkString(", ")}],[${v2.mkString(", ")}]")
        }
      }
      sc.stop
   }
}