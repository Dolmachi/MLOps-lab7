import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.mongodb.spark._
import org.apache.logging.log4j.{LogManager, Logger}

object DataMart {
  private val logger: Logger = LogManager.getLogger(getClass)

  // ── SparkSession с правильным пакетом коннектора ────────────────
  val spark: SparkSession = SparkSession.builder()
    .appName("DataMart")
    .master("local[*]")
    .config(
      "spark.mongodb.read.partitioner",
      "com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner"
    )
    .config("spark.mongodb.read.partitionerOptions.partitionSizeMB", "64")
    .config("spark.mongodb.read.sql.pipeline.includeFiltersAndProjections", "false")
    .config("spark.mongodb.read.pushdown.enabled", "false")
    .config("spark.mongodb.read.connection.uri",
          "mongodb://user:12345@mongodb:27017/products_database?authSource=admin")
    .config("spark.mongodb.write.connection.uri",
          "mongodb://user:12345@mongodb:27017/products_database?authSource=admin")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory",   "4g")
    .config("spark.executor.cores",  "4")
    .getOrCreate()

  def getRawData: DataFrame = {
    try {
      logger.info("Чтение данных из MongoDB: products_database.products")
      val df = spark.read
        .format("mongodb")
        .option("database", "products_database")
        .option("collection", "products_raw")
        .load()
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

      val nutrientCols = rawDF.columns.filter(_.endsWith("_100g"))

      val casted = nutrientCols.foldLeft(rawDF) { (df, c) =>
        df.withColumn(c, col(c).cast(DoubleType))
      }

      var processed = casted
        .select(nutrientCols.map(col): _*)
        .na.drop("all", nutrientCols)
        .na.fill(0.0)

      val medianExprs: Seq[Column] =
        nutrientCols.map(c => percentile_approx(col(c), lit(0.5), lit(10000)).alias(c))

      val medianRow = processed.agg(medianExprs.head, medianExprs.tail: _*).first()

      val medianMap: Map[String, Double] =
        nutrientCols.zip(medianRow.toSeq.map {
          case d: java.lang.Double => d.doubleValue() 
          case _                   => 0.0               
        }).toMap

      nutrientCols.foreach { c =>
        processed = processed.withColumn(
          c,
          when(col(c) < 0.0 || col(c) > 1000.0, lit(medianMap(c))).otherwise(col(c))
        )
      }

      logger.info("Предобработка завершена")
      processed
    } catch {
      case e: Exception =>
        logger.error("Ошибка при предобработке данных", e)
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