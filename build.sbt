name := "Stream_Query_Processor"

version := "1.0"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
    "org.apache.flink" %% "flink-scala" % "1.10.0",
    "org.apache.flink" % "flink-core" % "1.10.0",
    "org.apache.flink" %% "flink-streaming-scala" % "1.10.0",
    "org.apache.flink" %% "flink-connector-kafka" % "1.10.0",
    "org.apache.flink" % "flink-table" % "1.10.0" % "provided" pomOnly(),
    "org.apache.flink" %% "flink-table-api-scala" % "1.10.0",
    "org.apache.flink" %% "flink-table-planner" % "1.10.0",
    "org.apache.flink" %% "flink-table-api-scala-bridge" % "1.10.0",
    "org.apache.flink" % "flink-table-common" % "1.10.0" % "provided",
    "org.apache.flink" % "flink-csv" % "1.10.0"
)

