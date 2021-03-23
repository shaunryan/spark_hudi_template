  
name := "SparkHudiApp"

version := "0.1"

scalaVersion := "2.12.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",

  // hudi
  "org.apache.parquet" % "parquet-hive-bundle" % "1.11.1",
  "org.apache.hudi" %% "hudi-spark-bundle" % "0.7.0",
  "org.apache.spark" %% "spark-avro" % "3.1.1",

  // testing dependencies
  "org.scalactic" %% "scalactic" % "3.2.5",
  "org.scalatest" %% "scalatest" % "3.2.5" % "test"
)
