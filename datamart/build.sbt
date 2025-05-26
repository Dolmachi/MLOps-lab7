name := "DataMart"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.3",
  "org.apache.spark" %% "spark-mllib" % "3.4.3",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.0",
  "com.typesafe.akka" %% "akka-http" % "10.2.10",
  "com.typesafe.akka" %% "akka-stream" % "2.6.20",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.6.20",
  "de.heikoseeberger" %% "akka-http-circe" % "1.39.2",
  "io.circe" %% "circe-generic" % "0.14.6"
)