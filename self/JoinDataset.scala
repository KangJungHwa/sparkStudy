package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object JoinDataset {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(List("a","b","c","d","e")).map(x => (x,1))
      val rdd2=sc.parallelize(List("d","e","f","g","h")).map(x => (x,2))
      val rdd3= rdd1.leftOuterJoin(rdd2)
      rdd3.collect.foreach {
        case(k, (v1,v2)) => println(s"${k},[${v1},${v2}]")
      }
      
      val rdd4= rdd1.rightOuterJoin(rdd2)
      rdd4.collect.foreach {
        case(k, (v1,v2)) => println(s"${k},[${v1},${v2}]")
      }
      val rdd5= rdd1.join(rdd2)
      rdd5.collect.foreach {
        case(k, (v1,v2)) => println(s"${k},[${v1},${v2}]")
      }
      
      sc.stop
  }
}