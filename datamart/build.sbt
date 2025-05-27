name := "DataMart"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.3" exclude("org.slf4j", "slf4j-simple") exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-mllib" % "3.4.3" exclude("org.slf4j", "slf4j-simple") exclude("org.slf4j", "slf4j-log4j12"),
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.19.0",
  "org.slf4j" % "slf4j-api" % "2.0.6",
  "com.typesafe.akka" %% "akka-http" % "10.2.10" exclude("org.slf4j", "slf4j-simple") exclude("org.slf4j", "slf4j-log4j12"),
  "com.typesafe.akka" %% "akka-stream" % "2.6.20" exclude("org.slf4j", "slf4j-simple") exclude("org.slf4j", "slf4j-log4j12"),
  "com.typesafe.akka" %% "akka-actor-typed" % "2.6.20" exclude("org.slf4j", "slf4j-simple") exclude("org.slf4j", "slf4j-log4j12"),
  "de.heikoseeberger" %% "akka-http-circe" % "1.39.2",
  "io.circe" %% "circe-generic" % "0.14.6"
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.endsWith(".properties") => MergeStrategy.first
  case x if x.endsWith(".class") => MergeStrategy.first
  case PathList("org", "slf4j", "impl", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Укажем, что зависимости должны включаться в JAR
assembly / assemblyShadeRules := Seq()