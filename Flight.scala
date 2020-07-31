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
import org.apache.spark.sql.functions.{udf, col, lit}


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

  def mapAirport(mapping: Map[String, String])(wban: String): Option[String] = {
    mapping.get(wban)
  }

  def main(args: Array[String]) {
    val dataDir = args(0)
    val conf: SparkConf = new SparkConf().setAppName("FlightProject")
    implicit val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val year = "2017"
    val month = "{01,1}"
    val weatherFiles = dataDir + s"/${year}${month}hourly.txt"
    val flightFiles = dataDir + s"/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_${year}_${month}.csv"
    val airportCodesFile = dataDir + s"/airport_codes_map.txt"
    val stationFiles = dataDir + s"/${year}${month}station.txt"

    // get wban/airport mapping from station file
    val stationDf = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(stationFiles)
      .select("WBAN", "CallSign")
      .na.drop()
    val wbanToAirport = stationDf
      .collect()
      .map(r => (r.getString(0) -> r.getString(1)))
      .toMap

    // get wban/airport mapping from precomputed file
    val wbanToAirportDf = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(airportCodesFile)
    val wbanToAirport = wbanToAirportDf
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap

    var weatherDf = spark.read.format("csv")
      .option("header", "true")
      .load(weatherFiles)
    val mapAirportUdf = udf(mapAirport(wbanToAirport) _)
    weatherDf = weatherDf
      .withColumn("Airport", mapAirportUdf(col("WBAN")))
      .filter(!col("Airport").isNull)

    skyConditions.foreach { c =>
      val parseSkyConditionUdf = udf(parseSkyCondition(c) _)
      weather = weather.withColumn(s"SkyCondition.${c}", parseSkyConditionUdf(col("skyCondition")))
    }

    weatherTypes.foreach { w =>
      val parseWeatherTypeUdf = udf(parseWeatherType(w) _)
      weather = weather.withColumn(s"WeatherTypes.${w}", parseWeatherTypeUdf(col("WeatherType")))
    }

    var flightsDf = spark.read.format("csv")
      .option("header", "true")
      .load(flightFiles)

    // ignore flights diverted of cancelled
    flightsDf = flightsDf.filter((col("Cancelled") === 0.0 && col("Diverted") === 0.0))

    // add labels
    val threshold = 15
    flightsDf = flightsDf.withColumn("D1", col("CarrierDelay") === 0.0 && col("SecurityDelay") === 0.0 && col("LateAircraftDelay") === 0.0 && col("ArrDelay") >= lit(threshold))
    flightsDf = flightsDf.withColumn("D2", (col("WeatherDelay") > 0.0 && col("ArrDelay") >= lit(threshold)) || (col("NasDelay") >= lit(threshold) && !col("NasDelay").isNull))
    flightsDf = flightsDf.withColumn("D3", (col("ArrDelay") >= lit(threshold) && (col("WeatherDelay") > 0.0 || col("NasDelay") > 0.0)))
    flightsDf = flightsDf.withColumn("D4", col("ArrDelay") >= lit(threshold))
    flightsDf.persist()

    // negative subsampling sampling
    val label = "D2"
    val negativeSampleRate = 0.33
    val positivesDf = flightsDf.filter(col(label))
    val negativesDf = flightsDf.filter(!col(label)).sample(negativeSampleRate)
    flightsDf = positivesDf.union(negativesDf)


    spark.stop()
  }
}
