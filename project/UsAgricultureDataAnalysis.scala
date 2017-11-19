import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

object UsAgricultureDataAnalysis {
  import sbt.{url, Artifact, Def, ModuleID, ScalaVersion}
  import sbt.Keys._
  import sbtassembly.PathList
  import sbtassembly.AssemblyPlugin.autoImport.{assemblyMergeStrategy, MergeStrategy}

  val settings: Seq[Def.Setting[_]] = Seq(
   
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      artifact.name + "." + artifact.extension
    },
   
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", _) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    description := "Enron public email data analysis",
    organizationName := "IOCTL Solutions",
    organizationHomepage := Some(url("http://www.ioctl.co")),
    organization := "co.ioctl",
    parallelExecution in Test := false,
    resolvers += "apache-snapshots" at "http://repository.apache.org/snapshots/",
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-encoding", 
      "UTF-8"
    ),
    scalaVersion := "2.11.8",
    test in assembly := {},
    version := "0.1.0-SNAPSHOT"
  )

  object Dependencies {
    val dependencies: Seq[ModuleID] = Seq(
      "org.apache.spark" %% "spark-core" % "2.2.0",
      "org.apache.spark" %% "spark-hive" % "2.2.0" % "test",
      "org.apache.spark" %% "spark-sql" % "2.2.0",
      "com.databricks" %% "spark-xml" % "0.4.1",
      "org.slf4j" % "slf4j-simple" % "1.7.21",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test"
    )
  }

}
