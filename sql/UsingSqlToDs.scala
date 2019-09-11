package Spark.com.spark.sql
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.rdd.RDD
      
//case class Car(model: String, price: Int, job: String)

object UsingSqlToDs {
  def main(args : Array[String]){

      val conf = new SparkConf()
      conf.setMaster("local[*]").setAppName("UsingSqlToDs")
      val sc=new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("DatasetSample")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
      import spark.implicits._
     // val df= spark.createDataFrame(data)
     // val df1=df.take(3).map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]+100))
      spark.stop
   }
}

