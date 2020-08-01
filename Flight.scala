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
import org.apache.spark.sql.functions.{udf, col, lit, unix_timestamp}
import scala.collection.mutable


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

  case class WeatherData(skyCondition: String, weatherType: String)
  val weatherDataSchema = new StructType()
    .add("skyCondition", StringType)
    .add("weatherType", StringType)
  def findWeatherData(ts: mutable.WrappedArray[Long], skyCondition: mutable.WrappedArray[String], weatherType: mutable.WrappedArray[String], targetTs: Long): WeatherData = {
    if (ts == null) {
      WeatherData("", "")
    } else {
      (0 until ts.length)
        .filter(ts(_) <= targetTs)
        .sortBy(idx => -ts(idx))
        .headOption
        .map(idx => WeatherData(skyCondition(idx), weatherType(idx)))
        .getOrElse(WeatherData("", ""))
    }
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

    var flightsDf = spark.read.format("csv")
      .option("header", "true")
      .load(flightFiles)

    // ignore flights diverted of cancelled
    flightsDf = flightsDf.filter((col("Cancelled") === 0.0 && col("Diverted") === 0.0))

    // get the set of airport
    val airports = (flightsDf.select(col("Origin")).collect ++ flightsDf.select(col("Dest")).collect).map(r => r.getString(0)).toSet

    // // get wban/airport mapping from station file
    // val stationDf = spark.read.format("csv")
    //   .option("delimiter", "|")
    //   .option("header", "true")
    //   .load(stationFiles)
    //   .select("WBAN", "CallSign")
    //   .na.drop()
    // val wbanToAirport = stationDf
    //   .collect()
    //   .map(r => (r.getString(0) -> r.getString(1)))
    //   .filter(kv => airports.contains(kv._2))
    //   .toMap

    // get wban/airport mapping from precomputed file
    val wbanToAirportDf = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(airportCodesFile)
    val wbanToAirport = wbanToAirportDf
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap

    // read weather data
    var weatherDf = spark.read.format("csv")
      .option("header", "true")
      .load(weatherFiles)

    // get airport from wban in the weather data
    val mapAirportUdf = udf(mapAirport(wbanToAirport) _)
    weatherDf = weatherDf
      .withColumn("Airport", mapAirportUdf(col("WBAN")))
      .filter(!col("Airport").isNull)

    // skyConditions.foreach { c =>
    //   val parseSkyConditionUdf = udf(parseSkyCondition(c) _)
    //   weatherDf = weatherDf.withColumn(s"SkyCondition.${c}", parseSkyConditionUdf(col("skyCondition")))
    // }

    // weatherTypes.foreach { w =>
    //   val parseWeatherTypeUdf = udf(parseWeatherType(w) _)
    //   weatherDf = weatherDf.withColumn(s"WeatherTypes.${w}", parseWeatherTypeUdf(col("WeatherType")))
    // }

    // parse timestamps for weather
    weatherDf = weatherDf
      .withColumn("Time", lpad(col("Time"), 4, "0"))
      .withColumn("Ts", concat(col("date"), col("Time")))
      .withColumn("Ts", unix_timestamp(col("Ts"), "yyyyMMddHHmm"))
      .withColumn("Day", (col("Ts") / 86400).cast(IntegerType))

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
    val negativesDf = flightsDf.filter(!col(label)).sample(negativeSampleRate, seed=12345)
    flightsDf = positivesDf.union(negativesDf)

    // parse timestamps for flights
    flightsDf = flightsDf
      .withColumn("DepTime", lpad(col("DepTime"), 4, "0"))
      .withColumn("DepTime", when(col("DepTime").equalTo("2400"), "2359").otherwise(col("DepTime")))
      .withColumn("DepTs", unix_timestamp(concat(col("FlightDate"), col("DepTime")), "yyyy-MM-ddHHmm"))
      .withColumn("DepDay", (col("DepTs") / 86400).cast(IntegerType))
      .withColumn("ArrTime", lpad(col("ArrTime"), 4, "0"))
      .withColumn("ArrTime", when(col("ArrTime").equalTo("2400"), "2359").otherwise(col("ArrTime")))
      .withColumn("ArrTs", unix_timestamp(concat(col("FlightDate"), col("ArrTime")), "yyyy-MM-ddHHmm"))
      .withColumn("ArrDay", (col("ArrTs") / 86400).cast(IntegerType))

    weatherDf = weatherDf
      .union(weatherDf.withColumn("day", col("day") + 1))
    weatherDf = weatherDf
      .select("Airport", "Day", "Ts", "Skycondition", "WeatherType")
      .groupBy(col("Day"), col("Airport"))
      .agg(
        collect_list("Ts").as("Ts"),
        collect_list("SkyCondition").as("SkyCondition"),
        collect_list("WeatherType").as("WeatherType"))
    flightsDf = flightsDf.select("DepTs", "DepDay", "ArrTs", "ArrDay", "Origin", "Dest")
    var joinedDf = flightsDf.join(weatherDf, col("DepDay") === col("Day") && col("Origin") === col("Airport"), "left")
    // println(joinedDf.count)

    val findWeatherDataUdf = udf(findWeatherData _, weatherDataSchema)
    joinedDf = joinedDf.withColumn("WeatherData", findWeatherDataUdf(col("Ts"), col("SkyCondition"), col("WeatherType"), col("DepTs")))
    // joinedDf = joinedDf.select("WeatherData")
    joinedDf = joinedDf.persist()
    println(joinedDf.count)
    println(joinedDf.filter(col("WeatherData.skycondition") === "" && col("WeatherData.weatherType") === "").count)
    joinedDf.show(100)

    spark.stop()
  }
}
