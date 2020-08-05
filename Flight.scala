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

  def splitData(preparedDF: DataFrame, splitTrainingRatio: Double): (DataFrame, DataFrame) = {
    val Array(trainingDataDF, testDataDF) = preparedDF
      .randomSplit(Array(splitTrainingRatio, 1.0 - splitTrainingRatio))
    (trainingDataDF, testDataDF)
  }

  def trainModel(trainingDataDF: DataFrame): ClassificationModel[_, _] = {
    val classifier = new LogisticRegression().setMaxIter(10)
    classifier.fit(trainingDataDF)
  }

  def evaluateModel(testDataDF: DataFrame, model: ClassificationModel[_, _]) = {
    val predictions = model.transform(testDataDF)
    val getScore = udf((xs: org.apache.spark.ml.linalg.Vector) => xs.toArray(1))
    val predictionsWithScore = predictions.withColumn("score", getScore(col("probability"))).select("score", "label").rdd.map(x => (x.getDouble(0), x.getDouble(1)))
    val metrics = new BinaryClassificationMetrics(predictionsWithScore)
    val (bestThreshold, bestF1Score) = metrics.fMeasureByThreshold.collect().maxBy(_._2)
    val recall = metrics.recallByThreshold.collect().filter(x => x._1 == bestThreshold)(0)._2
    val precision = metrics.precisionByThreshold.collect().filter(x => x._1 == bestThreshold)(0)._2
    println(s"best F-score: ${bestF1Score}, threshold: ${bestThreshold}, precision: ${precision}, recall: ${recall}")
    println("Area under ROC = " + metrics.areaUnderROC)
  }

  def mapAirport(mapping: Map[String, String])(wban: String): Option[String] = {
    mapping.get(wban)
  }

  // case class WeatherData(skyCondition: String, weatherType: String)
  // val weatherDataSchema = new StructType()
  //   .add("skyCondition", StringType)
  //   .add("weatherType", StringType)
  // def findWeatherData(ts: mutable.WrappedArray[Long], skyCondition: mutable.WrappedArray[String], weatherType: mutable.WrappedArray[String], targetTs: Long): WeatherData = {
  //   if (ts == null) {
  //     WeatherData("", "")
  //   } else {
  //     (0 until ts.length)
  //       .filter(ts(_) <= targetTs)
  //       .sortBy(idx => -ts(idx))
  //       .headOption
  //       .map(idx => WeatherData(skyCondition(idx), weatherType(idx)))
  //       .getOrElse(WeatherData("", ""))
  //   }
  // }

  case class WeatherData(ts: Long, skyCondition: String, weatherType: String)
  val weatherDataSchema = ArrayType(StructType(Array(
          StructField("ts", LongType),
          StructField("skyCondition", StringType),
          StructField("weatherType", StringType)
  )))

  def fillWeatherData(minTs: Long, maxTs: Long)(ts: mutable.WrappedArray[Long], skyCondition: mutable.WrappedArray[String], weatherType: mutable.WrappedArray[String]): Seq[WeatherData] = {
    val sortedIdx = (0 until ts.length)
      .sortBy(ts(_))
    var currentSkyCondition = ""
    var currentWeatherType = ""
    var idx = 0
    (minTs until maxTs by 3600).map{ currentTs =>
      while (idx + 1 < ts.length && ts(sortedIdx(idx+1)) <= currentTs) {
        idx = idx + 1
      }
      currentSkyCondition = skyCondition(idx)
      currentWeatherType = weatherType(idx)
      WeatherData(currentTs, currentSkyCondition, currentWeatherType)
    }
  }


  def usage(): Unit = {
    println("usage: spark-submit [SPARK_CONF] --class \"FlightProject\" JAR_FILE [--dataDir DATA_DIR] [--year YEAR] [--month MONTH]")
  }


  def main(args: Array[String]) {
    var dataDir: String = "data/"
    var year: String = "2017"
    var month: String = "{01,1}"
    if (args.length % 2 == 1) {
      usage()
      return
    }
    args.sliding(2, 2).toList.collect {
      case Array("--dataDir", dataDirArg: String) => dataDir = dataDirArg
      case Array("--year", yearArg: String) => year = yearArg
      case Array("--month", monthArg: String) => month = monthArg
      case Array(_, _) => {
        usage()
        return
      }
    }


    val conf: SparkConf = new SparkConf().setAppName("FlightProject")
    implicit val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val weatherFiles = dataDir + s"/${year}${month}hourly.txt"
    val flightFiles = dataDir + s"/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_${year}_${month}.csv"
    val airportCodesFile = dataDir + s"/airport_codes_map.txt"
    val stationFiles = dataDir + s"/${year}${month}station.txt"

    var flightsDf = spark.read.format("csv")
      .option("header", "true")
      .load(flightFiles)
      // .sample(1.0, seed=12345)

    // ignore flights diverted of cancelled
    flightsDf = flightsDf
      .filter((col("Cancelled") === 0.0 && col("Diverted") === 0.0))
      .withColumn("FlightSeqId", monotonically_increasing_id())
      .repartition(200)

    // // get the set of airport
    // val airports = (flightsDf.select(col("Origin")).collect ++ flightsDf.select(col("Dest")).collect).map(r => r.getString(0)).toSet

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
      // .sample(1.0, seed=12345)
      .repartition(200)

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
    flightsDf = flightsDf
      .withColumn("D1", col("CarrierDelay") === 0.0 && col("SecurityDelay") === 0.0 && col("LateAircraftDelay") === 0.0 && col("ArrDelay") >= lit(threshold))
      .withColumn("D2", (col("WeatherDelay") > 0.0 && col("ArrDelay") >= lit(threshold)) || (col("NasDelay") >= lit(threshold) && !col("NasDelay").isNull))
      .withColumn("D3", (col("ArrDelay") >= lit(threshold) && (col("WeatherDelay") > 0.0 || col("NasDelay") > 0.0)))
      .withColumn("D4", col("ArrDelay") >= lit(threshold))
    flightsDf.persist()

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

    // weatherDf = weatherDf
    //   .union(weatherDf.withColumn("day", col("day") + 1))
    // weatherDf = weatherDf
    //   .select("Airport", "Day", "Ts", "Skycondition", "WeatherType")
    //   .groupBy(col("Day"), col("Airport"))
    //   .agg(
    //     collect_list("Ts").as("Ts"),
    //     collect_list("SkyCondition").as("SkyCondition"),
    //     collect_list("WeatherType").as("WeatherType"))
    // flightsDf = flightsDf.select("DepTs", "DepDay", "ArrTs", "ArrDay", "Origin", "Dest")
    // var joinedDf = flightsDf.join(weatherDf, col("DepDay") === col("Day") && col("Origin") === col("Airport"), "left")

    // val findWeatherDataUdf = udf(findWeatherData _, weatherDataSchema)
    // joinedDf = joinedDf.withColumn("WeatherData", findWeatherDataUdf(col("Ts"), col("SkyCondition"), col("WeatherType"), col("DepTs")))
    // // joinedDf = joinedDf.select("WeatherData")
    // joinedDf = joinedDf.persist()
    // println(joinedDf.filter(col("WeatherData.skycondition") === "" && col("WeatherData.weatherType") === "").count)

    // // group weather per station
    // //    output one line per hour

    // // join flights with weather of origin
    // //    one row per (flight, hour)
    // //    group by flight with collect_list

    // // join flights with weather of arrival

    // println(weatherDf.count)
    // // println(flightsDf.count())
    // println(joinedDf.count)
    // // flightsDf.show()

    val groupedWeatherDf = weatherDf
      .groupBy(col("Airport"))
      .agg(
        collect_list("Ts").as("Ts"),
        collect_list("SkyCondition").as("SkyCondition"),
        collect_list("WeatherType").as("WeatherType"))

    println("weatherDf.count", weatherDf.count)

    val minTs = 3600 * ((flightsDf.agg(min(col("DepTs"))).collect()(0)(0).asInstanceOf[Long] - 12 * 3600) / 3600)
    val maxTs = 3600 * (flightsDf.agg(max(col("ArrTs"))).collect()(0)(0).asInstanceOf[Long] / 3600)
    println("minTs", minTs)
    println("maxTs", maxTs)

    val fillWeatherDataUdf = udf(fillWeatherData(minTs, maxTs) _, weatherDataSchema)
    val filledWeatherDf = groupedWeatherDf
      .withColumn("weatherData", fillWeatherDataUdf(col("Ts"), col("SkyCondition"), col("WeatherType")))
      .withColumn("weatherData", explode(col("weatherData")))
      .select("Airport", "weatherData.*")
    println("filledWeatherDf.count", filledWeatherDf.count)

    val nbWeatherDataHours = 2
    val flightDepWeatherDf = flightsDf.join(filledWeatherDf,
      filledWeatherDf.col("Airport") === flightsDf.col("Origin") &&
        filledWeatherDf.col("Ts").between(
          flightsDf.col("DepTs") - nbWeatherDataHours * 3600, flightsDf.col("DepTs")))
      .groupBy(col("FlightSeqId"))
      .agg(
        min(col("Origin")).as("Origin"),
        min(col("Dest")).as("Dest"),
        min(col("DepTs")).as("DepTs"),
        min(col("DepDay")).as("DepDay"),
        min(col("ArrTs")).as("ArrTs"),
        min(col("ArrDay")).as("ArrDay"),
        min(col("D1")).as("D1"),
        min(col("D2")).as("D2"),
        min(col("D3")).as("D3"),
        min(col("D4")).as("D4"),
        collect_list(struct(col("Ts"), col("SkyCondition"), col("WeatherType"))).alias("DepWeatherInfoStructs"))
      .withColumn("weatherInfoDep", sort_array(col("DepWeatherInfoStructs"), true))
      .drop(col("DepWeatherInfoStructs"))
      .persist()

    println("flightDepWeatherDf.count", flightDepWeatherDf.count)
    flightDepWeatherDf.show(5)

    val flightWeatherDf = flightDepWeatherDf.join(filledWeatherDf,
      filledWeatherDf.col("Airport") === flightDepWeatherDf.col("Dest") &&
        filledWeatherDf.col("Ts").between(
          flightDepWeatherDf.col("ArrTs") - nbWeatherDataHours * 3600, flightDepWeatherDf.col("ArrTs")))
      .groupBy(col("FlightSeqId"))
      .agg(
        min(col("Origin")).as("Origin"),
        min(col("Dest")).as("Dest"),
        min(col("DepTs")).as("DepTs"),
        min(col("DepDay")).as("DepDay"),
        min(col("ArrTs")).as("ArrTs"),
        min(col("ArrDay")).as("ArrDay"),
        min(col("D1")).as("D1"),
        min(col("D2")).as("D2"),
        min(col("D3")).as("D3"),
        min(col("D4")).as("D4"),
        min(col("weatherInfoDep")).as("weatherInfoDep"),
        collect_list(struct(col("Ts"), col("SkyCondition"), col("WeatherType"))).alias("ArrWeatherInfoStructs"))
      .withColumn("weatherInfoArr", sort_array(col("ArrWeatherInfoStructs"), true))
      .drop(col("ArrWeatherInfoStructs"))
      .persist()

    println("flightWeatherDf.count", flightWeatherDf.count)
    flightWeatherDf.show(5)

    // negative subsampling sampling
    val label = "D2"
    val negativeSampleRate = 0.33
    val positivesDf = flightWeatherDf.filter(col(label))
    val negativesDf = flightWeatherDf.filter(!col(label)).sample(negativeSampleRate, seed=12345)
    val sampledFlightWeatherDf = positivesDf.union(negativesDf)

    val destIndexer = new StringIndexer()
      .setInputCol("Dest")
      .setOutputCol("DestIdx")
    val assembler = new VectorAssembler()
      .setHandleInvalid("skip")
      .setInputCols(Array("DestIdx"))
      .setOutputCol("featuresVector")
    val vectorIndexer = new VectorIndexer()
      .setInputCol("featuresVector")
      .setOutputCol("features")
      .setMaxCategories(300)
    val pipeline = new Pipeline().setStages(Array(destIndexer, assembler, vectorIndexer))
    val transformedDF = pipeline
      .fit(flightWeatherDf)
      .transform(sampledFlightWeatherDf)
      .select("features", label)
      .withColumn("label", col("D2").cast(DoubleType))
    val (trainingDataDF, testDataDF) = splitData(transformedDF, 0.8)
    trainingDataDF.persist()
    testDataDF.persist()

    val model = trainModel(trainingDataDF)
    evaluateModel(trainingDataDF, model)
    evaluateModel(testDataDF, model)

    spark.stop()
  }
}
