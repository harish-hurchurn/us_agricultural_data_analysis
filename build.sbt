name := "crop_progress"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

assemblyMergeStrategy in assembly := {
  case PathList("com", "databricks", "spark", xs@_*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

spName := "crop_progress"
sparkVersion := "2.2.0"
sparkComponents := Seq("core", "sql")
spAppendScalaVersion := true
spIncludeMaven := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-hive" % "2.2.0" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

