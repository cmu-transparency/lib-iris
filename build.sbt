scalaVersion := "2.11.11"

//libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
//libraryDependencies += "com.holdenkarau" %%
//                       "spark-testing-base" % "2.1.0_0.6.0" % "test"

resolvers += Resolver.sonatypeRepo("snapshots")

//libraryDependencies += "org.platanios" %% "tensorflow" % "0.1.0-SNAPSHOT"
//libraryDependencies += "org.platanios" %% "tensorflow-data" % "0.1.0-SNAPSHOT"
//libraryDependencies += "org.platanios" %% "tensorflow" % "0.1.0-SNAPSHOT" classifier "darwin-cpu-x86_64"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"


scalacOptions += "-deprecation"

lazy val commonSettings = Seq(
  //scalaVersion := "2.11.8",
  organization := "edu.cmu.spf",
  version      := "0.2",
  shellPrompt in ThisBuild := { state =>
    "sbt:" + Project.extract(state).currentRef.project + "> " },
  test in assembly := {}
)

lazy val root = Project(
  id   = "iris",
  base = file("."),
  settings = commonSettings ++ Seq(
    mainClass in assembly := Some("edu.cmu.spf.iris.DataStats"),
    description := "Transparency for machine learning in scala",
    fork in run := true,
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-core" % "2.3.0"),
      "org.apache.spark" %% "spark-mllib" % "2.3.0",
      "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
//      "com.databricks"   %% "spark-csv"   % "1.5.0"
    )
  )
)

// http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin
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
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "git.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}