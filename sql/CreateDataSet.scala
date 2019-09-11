package Spark.com.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
object CreateDataSet {
  def main(args : Array[String]){
    val spark = SparkSession
      .builder()
      .appName("DatasetSample")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
  //  val sc = spark.sparkContext

    import spark.implicits._
      val list=List(("1","2","3"),
                    ("4","5","6"),
                    ("7","8","9"))

    val ds = spark.createDataset(list)

    ds.show
   

  
      spark.stop
   }
}