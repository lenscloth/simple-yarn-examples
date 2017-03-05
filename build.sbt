name := "simple-yarn-app"

version := "1.0"

scalaVersion := "2.12.1"


// Dependencies for main
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.7.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1",
  "org.apache.hadoop" % "hadoop-yarn-client" % "2.7.1",
  "coop.plausible.nx" % "no-exceptions_2.11" % "1.0.1"  // Checked exceptions for Scala
)