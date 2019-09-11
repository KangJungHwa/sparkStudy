package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
    val data = Seq(("Math", 100L), ("Eng", 80L), ("Math", 50L), ("Eng", 70L), ("Eng", 90L))
    val rdd = sc.parallelize(data)
    val createCombiner = (v: Long) => Record(v)
    val mergeValue = (c: Record, v: Long) => c.add(v)
    val mergeCombiners = (c1: Record, c2: Record) => c1.add(c2)
    val result = rdd.combineByKey(createCombiner, mergeValue, mergeCombiners)
    println(result.collect.mkString(",\t"))    
  
      sc.stop
   }
}


  