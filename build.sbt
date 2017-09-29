libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
//libraryDependencies += "com.holdenkarau" %%
//                       "spark-testing-base" % "2.1.0_0.6.0" % "test"

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
      "org.apache.spark" %% "spark-core"  % "2.0.1",
      "org.apache.spark" %% "spark-mllib" % "2.0.1",
      "com.databricks"   %% "spark-csv"   % "1.5.0"
    )
  )
)
