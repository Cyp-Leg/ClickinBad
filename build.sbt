name := "ClickinBad"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.2",
    "org.apache.spark" %% "spark-sql" % "2.3.2",
    "org.apache.spark" %% "spark-mllib-local" % "2.3.2",
    "joda-time" % "joda-time" % "2.9.9",
    "org.apache.spark" %% "spark-mllib" % "2.3.2"
)