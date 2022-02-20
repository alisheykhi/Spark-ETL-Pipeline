name := "SparkETLTemplate"

version := "1.1.3"

scalaVersion := "2.11.12"

organization := "ir.brtech"

useCoursier := false


val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.ini4j" % "ini4j" % "0.5.4",
  "com.github.sbahmani" % "jalcal" % "1.4",
  "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { f =>
    f.data.getName.contains("spark-core") || f.data.getName.contains("spark-sql")
  }
}

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

publishConfiguration := publishConfiguration.value.withOverwrite(true)

publishTo := {
        val nexus = "nexus-rep-add"
        if (isSnapshot.value)
                Some("maven-snapshots" at nexus + "repository/maven-snapshots")
        else
                Some("maven-releases" at "nexus-rep-add/repository/MVN_JAR_HOSTED")

}

