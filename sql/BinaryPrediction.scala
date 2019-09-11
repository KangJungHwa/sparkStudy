package Spark.com.spark.sql
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.rdd.RDD

case class Article(id: Long, topic: String, text: String)

object BinaryPrediction {
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

    val articles = Seq(
    Article(0, "sci.math", "Hello, Math!"),
    Article(1, "alt.religion", "Hello, Religion!"),
    Article(2, "sci.physics", "Hello, Physics!"),
    Article(3, "sci.math", "Hello, Math Revised!"),
    Article(4, "sci.math", "Better Math"),
    Article(5, "alt.religion", "TGIF")).toDS
        
    val topic2Label: Boolean => Double = isSci => if (isSci) 1 else 0
    val toLabel = udf(topic2Label)
    val labelled = articles.withColumn("label", toLabel($"topic".like("sci%"))).cache
    
    val Array(trainDF, testDF) = labelled.randomSplit(Array(0.70, 0.30), seed=9L)
     trainDF.show
     testDF.show
    import org.apache.spark.ml.feature.RegexTokenizer
    val tokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    
    import org.apache.spark.ml.feature.HashingTF
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)  // it does not wire transformers -- it's just a column name
      .setOutputCol("features")
      .setNumFeatures(5000)
    
    import org.apache.spark.ml.classification.LogisticRegression
    val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.01)
    
    import org.apache.spark.ml.Pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    val model = pipeline.fit(trainDF)

    val trainPredictions = model.transform(trainDF)
    val testPredictions = model.transform(testDF)
    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
    
    import org.apache.spark.ml.param.ParamMap
    val evaluatorParams = ParamMap(evaluator.metricName -> "areaUnderROC")
    import org.apache.spark.ml.tuning.ParamGridBuilder
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(100, 1000))
      .addGrid(lr.regParam, Array(0.05, 0.2))
      .addGrid(lr.maxIter, Array(5, 10, 15))
      .build    
    import org.apache.spark.ml.tuning.CrossValidator
    import org.apache.spark.ml.param._
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setNumFolds(10)
    
    val cvModel = cv.fit(trainDF)      
    val cvPredictions = cvModel.transform(testDF)
    cvPredictions.select('topic, 'text, 'prediction).show
    evaluator.evaluate(cvPredictions, evaluatorParams)
    spark.stop
   }
}