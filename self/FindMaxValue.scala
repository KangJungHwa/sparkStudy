package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object FindMaxValue {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
        val employeeData = List(("Jack",1000.0),("Bob",2000.0),("Carl",7000.0),("Jack",2000.0),("Bob",3000.0),("Carl",4000.0))
      val employeeRDD = sc.makeRDD(employeeData)
      //fold를 사용하려면 시작 값이 필요합니다
      val dummyEmployee = ("dummy",0.0);
      //fold와 foldByKey의 차이는 key별로 그룹핑을 해서 그룹함수를 적용할 지 전체에 그룹함수를 적용할지의 차이이다.
      val maxSalaryEmployee = employeeRDD.fold(("dummy",0.0))((acc,acc2) =>  if(acc._2 < acc2._2) acc2 else acc)
      println("employee with maximum salary is"+maxSalaryEmployee)    
      sc.stop
   }
}