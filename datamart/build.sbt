name := "DataMart"
version := "1.0"
scalaVersion := "2.12.18"

// ──────────────────────────────────────────────────────────────────
// Зависимости
// ──────────────────────────────────────────────────────────────────
libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-sql"              % "3.4.3"
    exclude("org.slf4j", "slf4j-simple")
    exclude("org.slf4j", "slf4j-log4j12"),

  "org.apache.spark"  %% "spark-mllib"            % "3.4.3"
    exclude("org.slf4j", "slf4j-simple")
    exclude("org.slf4j", "slf4j-log4j12"),

  // Mongo-Spark connector 10.3.0
  "org.mongodb.spark" %% "mongo-spark-connector"  % "10.3.0",

  // Логирование
  "org.apache.logging.log4j" % "log4j-core"       % "2.24.1",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl"% "2.24.1",
  "org.slf4j"                % "slf4j-api"        % "2.0.16",

  // Akka-HTTP + Circe
  "com.typesafe.akka" %% "akka-http"              % "10.2.10"
    exclude("org.slf4j", "slf4j-simple")
    exclude("org.slf4j", "slf4j-log4j12"),
  "com.typesafe.akka" %% "akka-stream"            % "2.6.20"
    exclude("org.slf4j", "slf4j-simple")
    exclude("org.slf4j", "slf4j-log4j12"),
  "com.typesafe.akka" %% "akka-actor-typed"       % "2.6.20"
    exclude("org.slf4j", "slf4j-simple")
    exclude("org.slf4j", "slf4j-log4j12"),
  "de.heikoseeberger" %% "akka-http-circe"        % "1.39.2",
  "io.circe"          %% "circe-generic"          % "0.14.9"
)

// Spark 3.4.x требует scala-parser-combinators ≥ 2.4.0
dependencyOverrides +=
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"

mainClass in Compile := Some("DataMartServer")

// ──────────────────────────────────────────────────────────────────
// Сборка fat-jar через sbt-assembly
// ──────────────────────────────────────────────────────────────────
import sbtassembly.AssemblyPlugin.defaultUniversalScript

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @_*)     => MergeStrategy.concat
  case PathList("META-INF", _ @_*)                 => MergeStrategy.discard
  case "reference.conf" | "application.conf"       => MergeStrategy.concat
  case x if x.endsWith(".proto")                   => MergeStrategy.first
  case x if x.endsWith(".properties")              => MergeStrategy.first
  case x if x.endsWith(".class")                   => MergeStrategy.first
  case PathList("org", "slf4j", "impl", _ @_*)     => MergeStrategy.discard
  case PathList("org", "apache", "log4j", xs @_*) if xs.contains("Log4j2Plugins.dat") =>
                                                     MergeStrategy.first
  case _                                           => MergeStrategy.first
}

// включаем зависимости и сам Scala-runtime
assembly / assemblyOption := (assembly / assemblyOption).value
  .withIncludeScala(true)
  .withIncludeDependency(true)

assembly / fullClasspath := (Compile / fullClasspath).value
