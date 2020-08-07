import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, udf, unix_timestamp}

import scala.collection.mutable


object FlightProject {

  def readFlightData(flightFiles: String)(implicit sc: SparkContext, spark: SparkSession): DataFrame = {
    var flightsDf = spark.read.format("csv")
      .option("header", "true")
      .load(flightFiles)
    // .sample(0.01, seed=12345)

    // ignore flights diverted of cancelled
    flightsDf = flightsDf
      .filter((col("Cancelled") === 0.0 && col("Diverted") === 0.0))

    // compute departure timestamps
    flightsDf = flightsDf
      .withColumn("CRSDepTime", lpad(col("CRSDepTime"), 4, "0"))
      .withColumn("CRSDepTime", when(col("CRSDepTime").equalTo("2400"), "2359").otherwise(col("CRSDepTime")))
      .withColumn("DepTs", unix_timestamp(concat(col("FlightDate"), col("CRSDepTime")), "yyyy-MM-ddHHmm"))

    val computeNextDayFactor = udf((computedHour: Long, localTimeHour: Long) => {
      if (math.abs(localTimeHour - computedHour) > 6)  24L * 60L * 60L  // 1 day
      else 0L
    })

    // compute arrival timestamps
    flightsDf = flightsDf
      .withColumn("CRSArrTimeHour", col("CRSArrTime").substr(0, 2))
      .withColumn("CRSArrTimeMinute", col("CRSArrTime").substr(3, 2))
      .withColumn("ComputedArrDateTimeHourInSeconds", col("DepTs") + (col("CRSElapsedTime").cast(LongType) * 60L))
      .withColumn("ArrTs",
        col("DepTs") +
          col("CRSArrTimeHour").cast(LongType) * 3600 +
          col("CRSArrTimeMinute").cast(LongType) * 60 +
          computeNextDayFactor(
            hour(col("ComputedArrDateTimeHourInSeconds").cast(TimestampType)).cast(LongType),
            col("CRSARRTimeHour").cast(LongType)
          )
      )

    // select relevant columns
    flightsDf = flightsDf.select(
      monotonically_increasing_id().as("FlightSeqId"),
      col("DepTs"),
      col("ArrTs"),
      col("Origin"),
      col("Dest"),
      col("DayOfWeek").cast(LongType),
      (col("DepTs") % 86400).as("DepSecondOfDay"),
      (col("ArrTs") % 86400).as("ArrSecondOfDay"),
      dayofyear(to_date(col("FlightDate"))).as("DayOfYear"),
      col("CRSElapsedTime"),
      col("CarrierDelay"),
      col("SecurityDelay"),
      col("LateAircraftDelay"),
      col("ArrDelay"),
      col("WeatherDelay"),
      col("NasDelay"))

    flightsDf.repartition(200)
  }

  def readWeatherData(weatherFiles: String)(implicit sc: SparkContext, spark: SparkSession): DataFrame = {
    var weatherDf = spark.read.format("csv")
      .option("header", "true")
      .load(weatherFiles)
    // .sample(0.01, seed=12345)

    // parse timestamps
    weatherDf = weatherDf
      .withColumn("Time", lpad(col("Time"), 4, "0"))
      .withColumn("Ts", concat(col("date"), col("Time")))
      .withColumn("Ts", unix_timestamp(col("Ts"), "yyyyMMddHHmm"))

    weatherDf = weatherDf.select(
      col("Ts"),
      col("WBAN"),
      col("SkyCondition"),
      col("WeatherType"),
      col("Visibility").cast(DoubleType),
      col("WindSpeed").cast(DoubleType),
      col("RelativeHumidity").cast(DoubleType),
      col("WindDirection").cast(DoubleType),
      col("StationPressure").cast(DoubleType))

    // handle missing values
    weatherDf = weatherDf.na.fill("", Seq(
      "SkyCondition",
      "WeatherType"))
    weatherDf = weatherDf.na.fill(-1, Seq(
      "Visibility",
      "WindSpeed",
      "RelativeHumidity",
      "WindDirection",
      "StationPressure"))

    weatherDf.repartition(200)
  }

  def mapWbanToAirportFromStationFiles(weatherDf: DataFrame, stationFiles: String, airports: Set[String])(implicit sc: SparkContext, spark: SparkSession): DataFrame = {
    val stationDf = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(stationFiles)
      .select("WBAN", "CallSign")
      .na.drop()
    val wbanToAirport = stationDf
      .collect()
      .map(r => (r.getString(0) -> r.getString(1)))
      .filter(kv => airports.contains(kv._2))
      .toMap
    val mapAirportUdf = udf(mapAirport(wbanToAirport) _)
    weatherDf
      .withColumn("Airport", mapAirportUdf(col("WBAN")))
      .filter(!col("Airport").isNull)
  }

  def mapWbanToAirportFromMappingFile(weatherDf: DataFrame, airportCodesFile: String)(implicit sc: SparkContext, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val wbanToAirportDf = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load(airportCodesFile)
    val wbanToAirport = wbanToAirportDf
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap
    val mapAirportUdf = udf(mapAirport(wbanToAirport) _)
    weatherDf
      .withColumn("Airport", mapAirportUdf(col("WBAN")))
      .filter(!col("Airport").isNull)
  }

  def addLabels(flightsDf: DataFrame, threshold: Int): DataFrame = {
    flightsDf
      .withColumn(s"D1", col("CarrierDelay") === 0.0 && col("SecurityDelay") === 0.0 && col("LateAircraftDelay") === 0.0 && col("ArrDelay") >= lit(threshold))
      .withColumn(s"D2", (col("WeatherDelay") > 0.0 && col("ArrDelay") >= lit(threshold)) || (col("NasDelay") >= lit(threshold) && !col("NasDelay").isNull))
      .withColumn(s"D3", (col("ArrDelay") >= lit(threshold) && (col("WeatherDelay") > 0.0 || col("NasDelay") > 0.0)))
      .withColumn(s"D4", col("ArrDelay") >= lit(threshold))
  }

  def joinDatasets(flightsDf: DataFrame, weatherDf: DataFrame, nbWeatherDataHours: Int): DataFrame = {
    val groupedWeatherDf = weatherDf
      .groupBy(col("Airport"))
      .agg(collect_list(struct(
        col("Ts"),
        col("SkyCondition"),
        col("WeatherType"),
        col("Visibility"),
        col("WindSpeed"),
        col("RelativeHumidity"),
        col("WindDirection"),
        col("StationPressure"))
      ).alias("weatherInfo"))

    val minTs = 3600 * ((flightsDf.agg(min(col("DepTs"))).collect()(0)(0).asInstanceOf[Long] - 12 * 3600) / 3600)
    val maxTs = 3600 * (flightsDf.agg(max(col("ArrTs"))).collect()(0)(0).asInstanceOf[Long] / 3600)

    val fillWeatherDataUdf = udf(fillWeatherData(minTs, maxTs) _, weatherDataSchema)
    val filledWeatherDf = groupedWeatherDf
      .withColumn("weatherData", fillWeatherDataUdf(col("weatherInfo")))
      .withColumn("weatherData", explode(col("weatherData")))
      .select("Airport", "weatherData.*")

    val flightDepWeatherDf = flightsDf.join(filledWeatherDf,
      filledWeatherDf.col("Airport") === flightsDf.col("Origin") &&
        filledWeatherDf.col("Ts").between(
          flightsDf.col("DepTs") - nbWeatherDataHours * 3600, flightsDf.col("DepTs")))
      .groupBy(col("FlightSeqId"))
      .agg(
        min(col("Origin")).as("Origin"),
        min(col("Dest")).as("Dest"),
        min(col("DepTs")).as("DepTs"),
        min(col("ArrTs")).as("ArrTs"),
        min(col("DayOfWeek")).as("DayOfWeek"),
        min(col("DepSecondOfDay")).as("DepSecondOfDay"),
        min(col("ArrSecondOfDay")).as("ArrSecondOfDay"),
        min(col("DayOfYear")).as("DayOfYear"),
        min(col("CRSElapsedTime")).as("CRSElapsedTime"),
        min(col("D1")).as("D1"),
        min(col("D2")).as("D2"),
        min(col("D3")).as("D3"),
        min(col("D4")).as("D4"),
        collect_list(struct(
          col("Ts"),
          col("SkyCondition"),
          col("WeatherType"),
          col("Visibility"),
          col("WindSpeed"),
          col("RelativeHumidity"),
          col("WindDirection"),
          col("StationPressure"))).alias("DepWeatherInfoStructs"))
      .withColumn("weatherInfoDep", sort_array(col("DepWeatherInfoStructs"), true))
      .drop(col("DepWeatherInfoStructs"))

    val flightWeatherDf = flightDepWeatherDf.join(filledWeatherDf,
      filledWeatherDf.col("Airport") === flightDepWeatherDf.col("Dest") &&
        filledWeatherDf.col("Ts").between(
          flightDepWeatherDf.col("ArrTs") - nbWeatherDataHours * 3600, flightDepWeatherDf.col("ArrTs")))
      .groupBy(col("FlightSeqId"))
      .agg(
        min(col("Origin")).as("Origin"),
        min(col("Dest")).as("Dest"),
        min(col("DepTs")).as("DepTs"),
        min(col("ArrTs")).as("ArrTs"),
        min(col("DayOfWeek")).as("DayOfWeek"),
        min(col("DepSecondOfDay")).as("DepSecondOfDay"),
        min(col("ArrSecondOfDay")).as("ArrSecondOfDay"),
        min(col("DayOfYear")).as("DayOfYear"),
        min(col("CRSElapsedTime")).as("CRSElapsedTime"),
        min(col("D1")).as("D1"),
        min(col("D2")).as("D2"),
        min(col("D3")).as("D3"),
        min(col("D4")).as("D4"),
        min(col("weatherInfoDep")).as("weatherInfoDep"),
        collect_list(struct(
          col("Ts"),
          col("SkyCondition"),
          col("WeatherType"),
          col("Visibility"),
          col("WindSpeed"),
          col("RelativeHumidity"),
          col("WindDirection"),
          col("StationPressure"))).alias("ArrWeatherInfoStructs"))
      .withColumn("weatherInfoArr", sort_array(col("ArrWeatherInfoStructs"), true))
      .drop(col("ArrWeatherInfoStructs"))

    flightWeatherDf
  }

  def subsampleDataset(flightWeatherDf: DataFrame, label: String, negativeSampleRate: Double): DataFrame = {
    val positivesDf = flightWeatherDf.filter(col(label))
    val negativesDf = flightWeatherDf.filter(!col(label)).sample(negativeSampleRate, seed=12345)
    positivesDf.union(negativesDf)
  }

  val skyConditions = Seq("FEW", "SCT", "BKN", "OVC")
  def parseSkyCondition(skyCondition: String)(skyConditionString: String): Int = {
    val default = 999
    if (skyConditionString == "M") {
      default
    } else {
      val tokens = skyConditionString.split(" ")
      tokens.find(t => t.length >= 6 && t.substring(0, 3) == skyCondition).map(t => t.substring(3, 6).toInt).getOrElse(default)
    }
  }

  val weatherTypes = Seq("VC", "MI", "PR", "BC", "DR", "BL", "SH", "TS", "FZ", "RA", "DZ", "SN", "SG", "IC", "PL", "GR", "GS", "UP", "FG", "VA", "BR", "HZ", "DU", "FU", "SA", "PY", "SQ", "PO", "DS", "SS", "FC")
  def parseWeatherType(weatherType: String)(weatherTypeString: String): Int = {
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

  def transformDataset(flightWeatherDf: DataFrame, sampledFlightWeatherDf: DataFrame, label: String, nbWeatherHours: Int): DataFrame = {

    var df = flightWeatherDf

    Seq("Arr", "Dep").foreach { airport =>
      (0 until nbWeatherHours).foreach { hour =>
        skyConditions.foreach { c =>
          val parseSkyConditionUdf = udf(parseSkyCondition(c) _)
          df = df.withColumn(s"${airport}_SkyCondition_${hour}_${c}", parseSkyConditionUdf(col(s"weatherInfo${airport}")(hour)("skyCondition")))
        }
        weatherTypes.foreach { w =>
          val parseWeatherTypeUdf = udf(parseWeatherType(w) _)
          df = df.withColumn(s"${airport}_WeatherTypes_${hour}_${w}", parseWeatherTypeUdf(col(s"weatherInfo${airport}")(hour)("WeatherType")))
        }
        df = df.withColumn(s"${airport}_Visibility_${hour}", col(s"weatherInfo${airport}")(hour)("Visibility"))
        df = df.withColumn(s"${airport}_WindSpeed_${hour}", col(s"weatherInfo${airport}")(hour)("WindSpeed"))
        df = df.withColumn(s"${airport}_RelativeHumidity_${hour}", col(s"weatherInfo${airport}")(hour)("RelativeHumidity"))
        df = df.withColumn(s"${airport}_WindDirection_${hour}", col(s"weatherInfo${airport}")(hour)("WindDirection"))
        df = df.withColumn(s"${airport}_StationPressure_${hour}", col(s"weatherInfo${airport}")(hour)("StationPressure"))
      }
    }

    val destIndexer = new StringIndexer().setInputCol("Dest").setOutputCol("DestIdx")
    val originIndexer = new StringIndexer().setInputCol("Origin").setOutputCol("OriginIdx")
    val assembler = new VectorAssembler()
      .setHandleInvalid("skip")
      .setInputCols(Array(
        "DestIdx",
        "OriginIdx",
        "DepTs",
        "ArrTs",
        "DayOfWeek",
        "DepSecondOfDay",
        "ArrSecondOfDay",
        "DayOfYear") ++
        Seq("Arr", "Dep").flatMap { airport =>
          (0 until 1).flatMap { hour =>
            skyConditions.map { c => s"${airport}_SkyCondition_${hour}_${c}" } ++
            // weathertypes.map { c => s"${airport}_WeatherTypes_${hour}_${c}" } ++
            Seq(s"${airport}_Visibility_${hour}",
              s"${airport}_WindSpeed_${hour}",
              s"${airport}_RelativeHumidity_${hour}",
              s"${airport}_WindDirection_${hour}",
              s"${airport}_StationPressure_${hour}")
          }
        }
      )
      .setOutputCol("featuresVector")
    val vectorIndexer = new VectorIndexer()
      .setInputCol("featuresVector")
      .setOutputCol("features")
      .setMaxCategories(300)
    val pipeline = new Pipeline().setStages(Array(destIndexer, originIndexer, assembler, vectorIndexer))
    val transformedDF = pipeline
      .fit(df)
      .transform(df)
      .select("features", label)
      .withColumn("label", col(label).cast(DoubleType))
    transformedDF
  }

  def splitDataset(df: DataFrame, splitTrainingRatio: Double): (DataFrame, DataFrame) = {
    val Array(trainingDataDF, testDataDF) = df
      .randomSplit(Array(splitTrainingRatio, 1.0 - splitTrainingRatio))
    (trainingDataDF, testDataDF)
  }

  def trainModel(trainingDataDF: DataFrame)(implicit modelType: String): ClassificationModel[_, _] = {
    val classifier = modelType match {
      case "lr" => new LogisticRegression().setMaxIter(10)
      case "xgboost" => new XGBoostClassifier(
        Map("objective" -> "binary:logistic", "num_round" -> 5))
        .setFeaturesCol("features")
        .setLabelCol("label")
      case "gbt" => new GBTClassifier()
          .setLabelCol("label")
          .setFeaturesCol("features")
//          .setMaxBins(300)
//          .setMaxIter(150)
//          .setStepSize(0.1)
//          .setMaxDepth(5)
      case "dt" => new DecisionTreeClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
    }
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
    println("Area under ROC: " + metrics.areaUnderROC)
    val multiclassMetrics = new MulticlassMetrics(predictionsWithScore)
    println(s"Accuracy: ${multiclassMetrics.accuracy}")
    println(s"Confusion matrix:\n ${multiclassMetrics.confusionMatrix}")
  }

  def mapAirport(mapping: Map[String, String])(wban: String): Option[String] = {
    mapping.get(wban)
  }

  case class WeatherData(
    Ts: Long,
    SkyCondition: String,
    WeatherType: String,
    Visibility: Double,
    WindSpeed: Double,
    RelativeHumidity: Double,
    WindDirection: Double,
    StationPressure: Double)
  val weatherDataSchema = ArrayType(StructType(Array(
    StructField("Ts", LongType),
    StructField("SkyCondition", StringType),
    StructField("WeatherType", StringType),
    StructField("Visibility", DoubleType),
    StructField("WindSpeed", DoubleType),
    StructField("RelativeHumidity", DoubleType),
    StructField("WindDirection", DoubleType),
    StructField("StationPressure", DoubleType)
  )))

  def fillWeatherData(minTs: Long, maxTs: Long)(weatherInfo: mutable.WrappedArray[Row]): Seq[WeatherData] = {
    val sortedWeatherInfos = weatherInfo.map {
      case Row(
        ts: Long,
        skyCondition: String,
        weatherType: String,
        visibility: Double,
        windSpeed: Double,
        relativeHumidity: Double,
        windDirection: Double,
        stationPressure: Double
      ) => WeatherData(
        ts,
        skyCondition,
        weatherType,
        visibility,
        windSpeed,
        relativeHumidity,
        windDirection,
        stationPressure)
    }.sortBy(_.Ts)
    var idx = 0
      (minTs until maxTs by 3600).map{ currentTs =>
        while (idx + 1 < sortedWeatherInfos.length && sortedWeatherInfos(idx+1).Ts <= currentTs) {
          idx = idx + 1
        }
        val weatherInfo = sortedWeatherInfos(idx)
        WeatherData(
          currentTs,
          weatherInfo.SkyCondition,
          weatherInfo.WeatherType,
          weatherInfo.Visibility,
          weatherInfo.WindSpeed,
          weatherInfo.RelativeHumidity,
          weatherInfo.WindDirection,
          weatherInfo.StationPressure)
      }
  }

  def usage(): Unit = {
    println("usage: spark-submit [SPARK_CONF] --class \"FlightProject\" JAR_FILE [--dataDir DATA_DIR] [--year YEAR] [--month MONTH] [--nbWeatherHours NB_WEATHER_HOURS] [--label LABEL] [--threshold THRESHOLD] [--negativeSamplingRate NEGATIVE_SAMPLING_RATE] [--modelType MODEL_TYPE]")
  }


  def main(args: Array[String]) {
    var dataDir: String = "data/"
    var year: String = "2017"
    var month: String = "{01,1}"
    var nbWeatherHours: Int = 12
    var label = "D2"
    var threshold = 15
    var negativeSamplingRate = 0.33
    implicit var modelType = "lr"
    if (args.length % 2 == 1) {
      usage()
      return
    }
    args.sliding(2, 2).toList.collect {
      case Array("--dataDir", dataDirArg: String) => dataDir = dataDirArg
      case Array("--year", yearArg: String) => year = yearArg
      case Array("--month", monthArg: String) => month = monthArg
      case Array("--nbWeatherHours", nbWeatherHoursArg: String) => nbWeatherHours = nbWeatherHoursArg.toInt
      case Array("--label", labelArg: String) => label = labelArg
      case Array("--threshold", thresholdArg: String) => threshold = thresholdArg.toInt
      case Array("--negativeSamplingRate", negativeSamplingRateArg: String) => negativeSamplingRate = negativeSamplingRateArg.toDouble
      case Array("--modelType", modelTypeArg: String) => modelType = modelTypeArg
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

    var flightsDf = readFlightData(flightFiles)

    var weatherDf = readWeatherData(weatherFiles)

    val airportCodesFile = dataDir + s"/airport_codes_map.txt"
    weatherDf = mapWbanToAirportFromMappingFile(weatherDf, airportCodesFile)

    // val stationFiles = dataDir + s"/${year}${month}station.txt"
    // val airports = (flightsDf.select(col("Origin")).collect ++ flightsDf.select(col("Dest")).collect).map(r => r.getString(0)).toSet
    // weatherDf = mapWbanToAirportFromStationFiles(weatherDf, airportCodesFile)

    flightsDf = addLabels(flightsDf, threshold).persist()

    val flightWeatherDf = joinDatasets(flightsDf, weatherDf, nbWeatherHours).persist()

    val sampledFlightWeatherDf = subsampleDataset(flightWeatherDf, label, negativeSamplingRate).persist()

    val transformedDF = transformDataset(flightWeatherDf, sampledFlightWeatherDf, label, nbWeatherHours)

    val (trainingDataDF, testDataDF) = splitDataset(transformedDF, 0.8)
    trainingDataDF.persist()
    testDataDF.persist()

    val model = trainModel(trainingDataDF)
    evaluateModel(trainingDataDF, model)
    evaluateModel(testDataDF, model)

    spark.stop()
  }
}
