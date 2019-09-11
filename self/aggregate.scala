package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object aggregate {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val input = sc.parallelize(1 to 10,3) 
      val result=input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
          (x,y) => (x._1 + y._1, x._2 + y._2))
      println(result)    
      val avg = result._1 / result._2.toFloat
      println(avg)
      sc.stop
   }
}