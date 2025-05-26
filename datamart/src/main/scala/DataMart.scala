import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, when, expr}
import com.mongodb.spark._
import org.apache.spark.ml.PipelineModel

object DataMart {
  // Инициализация Spark-сессии
  val spark: SparkSession = SparkSession.builder()
    .appName("DataMart")
    .master("local[*]")
    .config("spark.mongodb.read.connection.uri", "mongodb://user:12345@mongodb:27017/products_database?authSource=admin")
    .config("spark.mongodb.write.connection.uri", "mongodb://user:12345@mongodb:27017/products_database?authSource=admin")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.cores", "4")
    .getOrCreate()

  // Чтение данных из MongoDB
  def getRawData: DataFrame = {
    spark.read
      .format("mongodb")
      .option("database", "products_database")
      .option("collection", "products")
      .load()
  }

  // Предобработка данных
  def preprocessData(rawDF: DataFrame): DataFrame = {
    val nutrientColumns = rawDF.columns.slice(88, rawDF.columns.length)
    var processedDF = rawDF.select(nutrientColumns.map(col): _*)
      .filter(!nutrientColumns.map(col(_).isNull).reduce(_ && _))

    processedDF = processedDF.na.fill(0.0)

    val medianExprs = nutrientColumns.map(c => expr(s"percentile_approx($c, 0.5)").alias(c))
    val medians = processedDF.agg(medianExprs.head, medianExprs.tail: _*).collect()(0).toSeq
    val medianMap = nutrientColumns.zip(medians).toMap

    nutrientColumns.foreach { c =>
      processedDF = processedDF.withColumn(
        c,
        when(col(c) < 0.0 || col(c) > 1000.0, medianMap(c)).otherwise(col(c))
      )
    }

    processedDF
  }

  // Сохранение предсказаний в MongoDB
  def savePredictions(predictionsDF: DataFrame): Unit = {
    predictionsDF.write
      .format("mongodb")
      .mode("append")
      .option("database", "products_database")
      .option("collection", "products_clusters")
      .save()
  }

  def stop(): Unit = {
    spark.stop()
  }
}