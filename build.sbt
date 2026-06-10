name := "flink-data-mesh-pipeline"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.18"  // Flink 1.18+ is built against Scala 2.12

val flinkVersion = "1.18.1"
val kafkaConnectorVersion = "3.0.2-1.18"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" % "flink-clients" % flinkVersion,
  "org.apache.flink" % "flink-connector-kafka" % kafkaConnectorVersion,
  "org.apache.flink" % "flink-json" % flinkVersion,
  "org.apache.flink" % "flink-avro" % flinkVersion,
  "org.apache.avro" % "avro" % "1.11.3",
  "io.spray" %% "spray-json" % "1.3.6",
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  // Test
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.apache.flink" % "flink-test-utils" % flinkVersion % Test,
  "org.apache.flink" % "flink-runtime-web" % flinkVersion % Test,
  "org.apache.flink" % "flink-tests" % flinkVersion % Test
)
