scalaVersion := "2.11.11"

//libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
//libraryDependencies += "com.holdenkarau" %%
//                       "spark-testing-base" % "2.1.0_0.6.0" % "test"

resolvers += Resolver.sonatypeRepo("snapshots")

//libraryDependencies += "org.platanios" %% "tensorflow" % "0.1.0-SNAPSHOT"
//libraryDependencies += "org.platanios" %% "tensorflow-data" % "0.1.0-SNAPSHOT"
//libraryDependencies += "org.platanios" %% "tensorflow" % "0.1.0-SNAPSHOT" classifier "darwin-cpu-x86_64"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"


scalacOptions += "-deprecation"

lazy val commonSettings = Seq(
  //scalaVersion := "2.11.8",
  organization := "edu.cmu.spf",
  version      := "0.1",
  shellPrompt in ThisBuild := { state =>
    "sbt:" + Project.extract(state).currentRef.project + "> " }
)

lazy val root = Project(
  id   = "iris",
  base = file("."),
  settings = commonSettings ++ Seq(
    description := "Transparency for machine learning in scala",
    fork in run := true,
    mainClass    := Some("TestClass"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"  % "2.2.0",
      "org.apache.spark" %% "spark-mllib" % "2.2.0",
      "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
//      "com.databricks"   %% "spark-csv"   % "1.5.0"
    )
  )
)
