name := "SparkWindowing"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.1" % "provided"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.1"

libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
