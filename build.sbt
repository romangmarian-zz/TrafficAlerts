name := "traffic-alerts"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4" % "provided",
  "org.apache.spark" % "spark-streaming_2.12" % "2.4.4",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4",
  "com.typesafe.play" %% "play-json" % "2.6.0-M7"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.8.5"