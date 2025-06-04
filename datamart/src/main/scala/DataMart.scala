import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.mongodb.spark._
import org.apache.logging.log4j.{LogManager, Logger}

object DataMart {
  private val logger: Logger = LogManager.getLogger(getClass)
  private val mongoUri =
    "mongodb://user:12345@mongodb:27017/products_database?authSource=admin"

  /*спарк процесс*/ 
  val spark: SparkSession = SparkSession.builder()
    .appName("DataMart")
    .master("local[*]")
    .config("spark.ui.port", "4041")
    .config("spark.mongodb.connection.uri", mongoUri)
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.default.parallelism", "50")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.mongodb.read.partitioner",
      "com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner"
    )
    .config("spark.mongodb.read.partitioner.options.partition.size", "256")
    .config("spark.mongodb.read.sql.pipeline.includeFiltersAndProjections", "true")
    .config("spark.mongodb.read.pushdown.enabled", "false")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores",  "4")
    .config("spark.driver.memory",   "6g")
    .config("spark.driver.memoryOverhead",  "2g")
    .config("spark.memory.storageFraction", "0.5")
    .config("spark.mongodb.schema.sampleSize", "200")
    .getOrCreate()

  /*считывание данных из монго*/ 
  def getRawData: DataFrame = {
    try {
      println("Чтение данных из MongoDB: products_database.products")
      val df = spark.read
        .format("mongodb")
        .option("uri", mongoUri)
        .option("database", "products_database")
        .option("collection", "products_raw")
        .load()

      val wanted: Seq[Column] = Seq(col("_id")) ++ df.columns.filter(_.endsWith("_100g")).map(col)
      df.select(wanted: _*)
    } catch {
      case e: Exception =>
        println(s"Ошибка при чтении данных из MongoDB: ${e.getMessage}", e)
        throw e
    }
  }

  /*препроцесс данных*/ 
  def preprocessData(rawDF: DataFrame): DataFrame = {
    try {
      println("Начало предобработки данных...")

      val nutrientCols: Array[String] = rawDF.columns.filter(_.endsWith("_100g"))

      val casted: DataFrame = nutrientCols.foldLeft(rawDF) { case (df, c) =>
        df.withColumn(c, col(c).cast(DoubleType))
      }

      var processed: DataFrame = casted
        .na.drop("all", nutrientCols)
        .na.fill(0.0, nutrientCols)

      val medianAggs: Seq[Column] = nutrientCols.map { c =>
        percentile_approx(col(c), lit(0.5), lit(1000)).alias(c)
      }
      val medianVals: Array[Double] = processed
        .agg(medianAggs.head, medianAggs.tail: _*)
        .first()
        .toSeq
        .map(_.asInstanceOf[Double])
        .toArray

      val medians: Map[String, Double] = nutrientCols.zip(medianVals).toMap

      nutrientCols.foreach { c =>
        processed = processed.withColumn(
          c,
          when(col(c) < 0.0 || col(c) > 1000.0, lit(medians(c)))
            .otherwise(col(c))
        )
      }

      println("Предобработка завершена")
      processed
    } catch {
      case e: Exception =>
        logger.error("Ошибка при предобработке данных", e)
        throw e
    }
  }

  /*сохранение предсказаний в монго*/ 
  def savePredictions(predictionsDF: DataFrame): Unit = {
    try {
      println("Сохранение предсказаний в MongoDB: products_database.products_clusters")
      predictionsDF.write
        .format("mongodb")
        .mode("append")
        .option("uri", mongoUri)
        .option("database", "products_database")
        .option("collection", "products_clusters")
        .save()
      println("Предсказания успешно сохранены")
    } catch {
      case e: Exception =>
        println(s"Ошибка при сохранении предсказаний: ${e.getMessage}", e)
        throw e
    }
  }

  def stop(): Unit = {
    println("Остановка Spark-сессии")
    spark.stop()
  }
}