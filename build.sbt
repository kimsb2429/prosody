name := "spark-engine"

version := "0.0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "com.github.scopt" %% "scopt" % "4.0.0-RC2",
    "org.scala-lang" % "scala-reflect" % "2.11.8"
)
// provide the class path for invoking the jar
mainClass in assembly := Some("Driver.MainApp")

// exclude scala since it comes with spark
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// naming convention for jar
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
        case x => MergeStrategy.first
}
