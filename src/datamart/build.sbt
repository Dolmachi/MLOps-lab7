ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "DataMart",
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.3" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.4.3" % "provided",
      "org.mongodb.spark" % "mongo-spark-connector_2.12" % "10.2.0"
    )
  )