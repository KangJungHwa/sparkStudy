package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object AvgTest {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val scores = List(("math",100F), ("math",50F), ("eng",50F),("eng",60F), ("eng",90F))
      
      type sumScore_cnt = (Float, Int)
      type key_SumScore_cnt = (String, (Float, Int))
     
      val rdd = sc.parallelize(scores)
      
      //컴바이너 생성
      val combiner = (score:Float) => (score,1)
      
      //파티션내 계산방법 정의
      val mergeValue = (sum1:sumScore_cnt, score:Float) =>{  
        (sum1._1 + score, sum1._2 + 1)
      }
      
      //파티션간 계산방법 정의
      val mergeCombiners = (col1:sumScore_cnt , col2:sumScore_cnt)=>{
        (col1._1 + col2._1 , col1._2 + col2._2)
      }
      
       val rdd2=rdd.combineByKey(combiner, mergeValue, mergeCombiners).map({
         case(k, (s, c)) => (k, s/c)
       })
       
      rdd2.collect().foreach(println)      
      
      
//      
//      val rdd1=sc.parallelize(scores)
//      val rdd2=rdd1.map({
//        case(k,v)=>(k,(v,1))
//      })
//      
//      
//      val initval= ((0.0F,0))
//      val result=rdd2.aggregateByKey(initval)(
//          (ac: (Float,Int), ac1: (Float,Int)) => (ac._1 + ac1._1,  ac._2 +ac1._2), //ScoreAvg 타입에서 어떤  계산과정을 거쳐 key value로 표현을 할지
//          (ac1: (Float,Int),ac2: (Float,Int)) => (ac1._1 +ac2._1, ac1._2 + ac2._2) // 현재row와 이전row의 관계를 표현함
//          ).map({
//            case(key,value) => (key, value._1 / value._2)
//          })
//        result.collect.foreach(println)       
      
//      val rdd = sc.parallelize(scores) 
//      val rdd2=rdd.mapValues(value => (value, 1)) 
//      .reduceByKey {
//        case ((sumL, countL), (sumR, countR)) => 
//          (sumL + sumR, countL + countR)
//      }
//      .mapValues { 
//        case (sum , count) => sum / count 
//      }
//      rdd2.collect.foreach(println)
      
      //      val rdd = sc.parallelize(scores).map({
//        case(key,value) =>(key,(value,1))
//      })
//     val rdd2= rdd.reduceByKey({
//       case((value,cnt), (value1,cnt1)) =>(value+value1,cnt+cnt1) 
//     })
//     val rdd3= rdd2.map({
//       case(k, (v,c)) => (k, v/c )
//     })
//     rdd3.collect.foreach(println)
      sc.stop
   }
}
