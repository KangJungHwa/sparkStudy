package Spark.com.spark.self
import org.apache.spark.{SparkConf, SparkContext}

object FindMaxValueGroupBy {
  def main(args : Array[String]){
      val gsc=new GetSparkContext
      val sc= gsc.getSparkContext()
      val deptEmployees = List(
      ("cs",("jack",1000.0)),
      ("cs",("bron",1200.0)),
      ("phy",("sam",2200.0)),
      ("phy",("ronaldo",500.0))
    )
  val employeeRDD = sc.makeRDD(deptEmployees)

  val maxByDept = employeeRDD.foldByKey(("dummy",0.0))((acc,acc1)=> if(acc._2 > acc1._2) acc else acc1)
  
  println("maximum salaries in each dept" + maxByDept.collect().toList)
  
  
//        val employeeData = List(("Jack",1000.0),("Bob",2000.0),("Carl",7000.0))
//      val employeeRDD = sc.makeRDD(employeeData)
//      //fold를 사용하려면 시작 값이 필요합니다
//      val dummyEmployee = ("dummy",0.0);
//      val maxSalaryEmployee = employeeRDD.fold(("dummy",0.0))((acc,employee) =>  if(acc._2 < employee._2) employee else acc)
//      println("employee with maximum salary is"+maxSalaryEmployee) 
  
  
      sc.stop
   }
}