import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, when, expr}
import com.mongodb.spark._
import org.apache.logging.log4j.{LogManager, Logger}

object DataMart {
  private val logger: Logger = LogManager.getLogger(getClass)

  val spark: SparkSession = SparkSession.builder()
    .appName("DataMart")
    .master("local[*]")
    .config("spark.mongodb.read.connection.uri", "mongodb://user:12345@mongodb:27017/products_database?authSource=admin")
    .config("spark.mongodb.write.connection.uri", "mongodb://user:12345@mongodb:27017/products_database?authSource=admin")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.cores", "4")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0")
    .getOrCreate()

  def getRawData: DataFrame = {
    try {
      logger.info("Чтение данных из MongoDB: products_database.products")
      val df = spark.read
        .format("mongodb")
        .option("database", "products_database")
        .option("collection", "products")
        .load()
      logger.info(s"Получено ${df.count()} строк из MongoDB")
      df
    } catch {
      case e: Exception =>
        logger.error(s"Ошибка при чтении данных из MongoDB: ${e.getMessage}", e)
        throw e
    }
  }

  def preprocessData(rawDF: DataFrame): DataFrame = {
    try {
      logger.info("Начало предобработки данных...")
      val nutrientColumns = rawDF.columns.slice(88, rawDF.columns.length)
      logger.info(s"Выбраны столбцы: ${nutrientColumns.mkString(", ")}")

      var processedDF = rawDF.select(nutrientColumns.map(col): _*)
        .filter(!nutrientColumns.map(col(_).isNull).reduce(_ && _))
      logger.info(s"После фильтрации: ${processedDF.count()} строк")

      processedDF = processedDF.na.fill(0.0)
      logger.info("Пропущенные значения заполнены нулями")

      val medianExprs = nutrientColumns.map(c => expr(s"percentile_approx($c, 0.5)").alias(c))
      val medians = processedDF.agg(medianExprs.head, medianExprs.tail: _*).collect()(0).toSeq
      val medianMap = nutrientColumns.zip(medians).toMap
      logger.info(s"Медианы: ${medianMap.toString()}")

      nutrientColumns.foreach { c =>
        processedDF = processedDF.withColumn(
          c,
          when(col(c) < 0.0 || col(c) > 1000.0, medianMap(c)).otherwise(col(c))
        )
      }
      logger.info("Предобработка завершена")
      processedDF
    } catch {
      case e: Exception =>
        logger.error(s"Ошибка при предобработке данных: ${e.getMessage}", e)
        throw e
    }
  }

  def savePredictions(predictionsDF: DataFrame): Unit = {
    try {
      logger.info("Сохранение предсказаний в MongoDB: products_database.products_clusters")
      predictionsDF.write
        .format("mongodb")
        .mode("append")
        .option("database", "products_database")
        .option("collection", "products_clusters")
        .save()
      logger.info("Предсказания успешно сохранены")
    } catch {
      case e: Exception =>
        logger.error(s"Ошибка при сохранении предсказаний: ${e.getMessage}", e)
        throw e
    }
  }

  def stop(): Unit = {
    logger.info("Остановка Spark-сессии")
    spark.stop()
  }
}