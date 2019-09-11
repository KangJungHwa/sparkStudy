package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object PartitionRDD {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
//      val rdd=sc.textFile("C:\\dataset\\apache.txt").flatMap(_.split(" ")).filter(x => x.nonEmpty).map(x=>(x,1)).reduceByKey(_+_)
//      val rdd1= rdd.map(x=>x.swap).sortByKey(ascending=false).map(x=>x.swap)
//      val rdd2=rdd1.filter({
//          case(x, y) =>(y>10)
//      })
//      val rdd=sc.textFile("C:\\dataset\\1.txt").flatMap(_.split(" "))
//      println(rdd.collect.mkString(", "))
//      val rdd1=sc.textFile("C:\\dataset\\1.txt").flatMap(_.split(" "))
//      println(rdd1.collect.mkString(", "))
      
    var apache_licence=sc.textFile("c:\\dataset\\apache.txt")
    var filter=apache_licence.filter(x => !x.contains("LICEN"))

      
      
      sc.stop
   }
}