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

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case _ => MergeStrategy.last
}

mainClass in assembly := Some("Main")
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
assemblyOutputPath in assembly := file(s"./${name.value}-${version.value}.jar")