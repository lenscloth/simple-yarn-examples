name := "SimpleYarn"

version := "1.0"

scalaVersion := "2.11.8"

// Dependencies for main
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3",
  "org.apache.hadoop" % "hadoop-yarn-client" % "2.7.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", ps @ _*) => MergeStrategy.first
  case x => MergeStrategy.first
}
