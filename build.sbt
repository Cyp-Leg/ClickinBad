name := "ClickinBad"

version := "1.0"

scalaVersion := "2.11.0"

val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib-local" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "joda-time" % "joda-time" % "2.9.9"
)

mainClass in assembly := Some("Main")
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
assemblyOutputPath in assembly := file(s"./${name.value}-${version.value}.jar")