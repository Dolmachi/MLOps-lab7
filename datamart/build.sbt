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

// Принудительно указываем версию scala-parser-combinators
dependencyOverrides += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"

// Стратегия слияния для sbt-assembly
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard // Игнорируем все файлы в META-INF
  case "reference.conf" => MergeStrategy.concat // Конкатенируем файлы reference.conf
  case "application.conf" => MergeStrategy.concat // Конкатенируем application.conf
  case x if x.endsWith(".proto") => MergeStrategy.first // Берем первый файл для .proto
  case x if x.endsWith(".properties") => MergeStrategy.first // Берем первый файл для .properties
  case x if x.endsWith(".class") => MergeStrategy.first // Берем первый файл для .class
  case x => MergeStrategy.first // Для всех остальных файлов берем первый
}