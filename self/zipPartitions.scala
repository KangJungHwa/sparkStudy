package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object zipPartitions {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val rdd1=sc.parallelize(1 to 20, 3)
      val rdd2=sc.parallelize(List("a","b","c","d","e"), 3)
      
     val rdd3=rdd1.zipPartitions(rdd2, true)((it1, it2) => {
          it1.zipAll(it2, -1, "empty")
          .map({case(x1,x2)=>x1+"-"+x2})
     })
      
      //zipAll 함수는 iterator 결합하는데 사용한다. 컬렉션의 크기가 달라도 결합이 가능하다.
      //첫번째 iterator가 두번째 보다 요소가 더많으면 zipAll함수는 첫번째 iterator
      //의 나머지 요소에 empty라는 일종의 더미값을 결합한다.
      //2번째 요소가 더많으면 -1을 사용한다.
     
      
//      val rdd3=rdd1.zipPartitions(rdd2){
//        (it1, it2) => for{
//          v1 <- it1;
//          v2 <- it2
//        } yield v1 + v2
//      }
      rdd3.collect.foreach(println)
      sc.stop
   }
}