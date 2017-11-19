package co.ioctl.us_crop_data_processor

import org.apache.spark.sql.SparkSession

class AnalyseSoyBeanData(sparkSession: SparkSession) {
  import org.apache.spark.sql.DataFrame

  def createDataframe(inputFile: String): DataFrame = {
    val df = sparkSession.read.csv(inputFile)
      .drop("_c0")
      .drop("_c1")
      .na.drop()
    
    df.show()
    df
  }
  
  def createStateColumn(df: DataFrame) = {
    df.select("_c2")
      .toDF("State")
      .orderBy("State")
  }
}

object AnalyseSoyBeanData extends App {
  import org.apache.spark.{SparkConf, SparkContext}

  val sparkConf = new SparkConf()
    .setAppName("Enron average word count")
    .setMaster("local[*]")
  
  val sc = new SparkContext(sparkConf)

  val spark = SparkSession
    .builder
    .appName("Soybean analysis")
    .getOrCreate()

  val soybeanAnalysis = new AnalyseSoyBeanData(spark)

  val df = soybeanAnalysis.createDataframe(args(0))
  
  val stateDf = soybeanAnalysis.createStateColumn(df)

  sc.stop()
}
