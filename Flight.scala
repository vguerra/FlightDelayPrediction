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
import org.apache.spark.sql.functions.{udf, col}


object FlightProject {

  val skyConditions = Seq("FEW", "SCT", "BKN", "OVC")
  def parseSkyCondition(skyCondition: String)(skyConditionString: String): Int = {
    val default = 999
    if (skyConditionString == "M") {
      default
    } else {
      val tokens = skyConditionString.split(" ")
      tokens.find(t => t.substring(0, 3) == skyCondition).map(t => t.substring(3, 6).toInt).getOrElse(default)
    }
  }

  val weatherTypes = Seq("VC", "MI", "PR", "BC", "DR", "BL", "SH", "TS", "FZ", "RA", "DZ", "SN", "SG", "IC", "PL", "GR", "GS", "UP", "FG", "VA", "BR", "HZ", "DU", "FU", "SA", "PY", "SQ", "PO", "DS", "SS", "FC")
  def parseWeatherType(weatherType: String)(weatherTypeString: String): Int = {
    val default = 0
    val idx = weatherTypeString.indexOfSlice(weatherType)
    if (idx == -1) {
      0
    } else {
      if (idx != 0 && weatherTypeString(idx - 1) == '-')
        1
      else if (idx != 0 && weatherTypeString(idx - 1) == '+')
        3
      else
        2
    }
  }


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

    var weather = spark.read.format("csv")
      .option("header", "true")
      .load(weatherFiles)
    weather = weather.select("SkyCondition", "WeatherType")

    skyConditions.foreach { c =>
      val parseSkyConditionUdf = udf(parseSkyCondition(c) _)
      weather = weather.withColumn(s"SkyCondition.${c}", parseSkyConditionUdf(col("skyCondition")))
    }

    weatherTypes.foreach { w =>
      val parseWeatherTypeUdf = udf(parseWeatherType(w) _)
      weather = weather.withColumn(s"WeatherTypes.${w}", parseWeatherTypeUdf(col("WeatherType")))
    }
    weather.show(100)

    spark.stop()
  }
}
