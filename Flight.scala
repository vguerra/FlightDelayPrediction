// spark-submit --master yarn --conf spark.driver.cores=12 --class "FlightProject" target/scala-2.11/flightproject_2.11-1.0.jar

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD


object FlightProject {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("FlightProject")
    implicit val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val year = "2017"
    val month = "{01,1}"
    val weatherFiles = s"/user/vm.guerramoran/flights_data/${year}${month}hourly.txt"
    val flightFiles = s"/user/vm.guerramoran/flights_data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_${year}_${month}.csv"
    val airportCodesFile = s"/user/vm.guerramoran/flights_data/airport_codes_map.txt"

    val weather = spark.read.format("csv")
      .option("header", "true")
      .load(weatherFiles)
    weather.show()

    spark.stop()
  }
}
