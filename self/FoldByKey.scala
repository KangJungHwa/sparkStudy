package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}
//작성 완료
object FoldByKey {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.textFile("c:\\dataset\\apache.txt")
      val rdd2=rdd1.flatMap(x=>x.split(" ")).map((_,1)).foldByKey(0)(_ + _).map(x=>x.swap).sortByKey(ascending=false)
      val rdd3=rdd2.map(x =>x.swap)
      rdd3.collect.foreach(println)
      sc.stop
   }
}