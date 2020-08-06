// sbt package

name := "FlightProject"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"
// Check releases of xgboost here: https://repo1.maven.org/maven2/ml/dmlc/xgboost4j-spark_2.11
val xgboostVersion = "1.0.0"

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
resolvers += "XGBoost4J Release Repo" at "https://s3-us-west-2.amazonaws.com/xgboost-maven-repo/release/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "ml.dmlc" %% "xgboost4j" % xgboostVersion,
  "ml.dmlc" %% "xgboost4j-spark" % xgboostVersion
)
