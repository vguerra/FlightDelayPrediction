// sbt package

name := "FlightProject"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"
// Check releases of xgboost here: https://repo1.maven.org/maven2/ml/dmlc/xgboost4j-spark_2.11
val xgboostVersion = "1.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "ml.dmlc" %% "xgboost4j" % xgboostVersion,
  "ml.dmlc" %% "xgboost4j-spark" % xgboostVersion
)
