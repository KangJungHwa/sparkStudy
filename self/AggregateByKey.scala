package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val score=List(ScoreAvg("math",100),ScoreAvg("math",80),ScoreAvg("eng",100),ScoreAvg("eng",80),ScoreAvg("eng",60))
      val scoreWithKey=for(i<-score) yield(i.subject,i)
      val rdd1=sc.parallelize(scoreWithKey)
      val initval= ((0.0F,0))
      val result=rdd1.aggregateByKey(initval)(
          (ac: (Float,Int), x: ScoreAvg) => (ac._1 + x.score, ac._2 +1), //ScoreAvg 타입에서 어떤  계산과정을 거쳐 key value로 표현을 할지
          (ac1: (Float,Int),ac2: (Float,Int)) => (ac1._1 +ac2._1, ac1._2 + ac2._2) // 현재row와 이전row의 관계를 표현함
          ).map({
            case(key,value) => (key, value._1 / value._2)
          })
        result.collect.foreach(println)          
      sc.stop
   }
}