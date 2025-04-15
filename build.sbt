ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "PracticaBD-processing"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.apache.spark" %% "spark-hive" % "3.4.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.10"

)