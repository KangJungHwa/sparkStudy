package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.HashPartitioner

object Random {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val r=scala.util.Random
      
      val data = for(i <- 1 to 10) yield(r.nextInt(100),"-")
      val rdd1=sc.parallelize(data)
      rdd1.collect.foreach(println)
      val rdd2 = rdd1.repartitionAndSortWithinPartitions(new HashPartitioner(3))
      rdd2.foreachPartition(it => { 
           println("=========================");
           it.foreach({
            case(k, v)=>println(k)
            })
           }
           
      )
      sc.stop
   }
}