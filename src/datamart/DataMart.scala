import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import java.util.{Properties}
import java.io.FileInputStream
import scala.jdk.CollectionConverters._

object DataMart {

   /** Куда сохраняем витрину (общий Docker‑volume) */
  private val OUT_PATH = "/vitrine/products_mart.parquet"

  /** Где лежит ini‑файл внутри контейнера (копируем в Dockerfile) */
  private val CONFIG_INI = "/app/config.ini"

  def main(args: Array[String]): Unit = {

    // ── Считываем конфиг для спарка ────────────────────────────────
    val iniProps = new Properties()
    iniProps.load(new FileInputStream(CONFIG_INI))

    val sparkPairs: Map[String, String] =
      iniProps.asScala.collect { case (k, v) if k.startsWith("spark.") =>
        k -> v
      }.toMap

    // ── Создаем SparkSession ────────────────────────────────────────────
    var builder = SparkSession.builder()
      .appName("DataMartBuilder")
      .master("local[*]")

    sparkPairs.foreach { case (k, v) => builder = builder.config(k, v) }

    val spark = builder.getOrCreate()

    // ── Получаем исходные данные из Mongo ────────────────────────────────
    val dfRaw = spark.read
      .format("mongodb")
      .option("database", "products_database")
      .option("collection", "products_raw")
      .load()

    println(s"[DataMart] RAW docs: ${dfRaw.count}")

    // ── Препроцессинг  ───────────────────────────────────────────────────
    val nutrients = dfRaw.columns.drop(88)          // только нутриенты
    var df = dfRaw.select(nutrients.map(col): _*)
      .na.drop("all")
      .na.fill(0.0)

    val LOWER = 0.0; val UPPER = 1000.0
    val med = df.stat.approxQuantile(df.columns, Array(0.5), 0.0)
    df.columns.zip(med).foreach { case (c, m) =>
      val median = m.headOption.getOrElse(0.0)
      df = df.withColumn(
        c, when(col(c) < LOWER || col(c) > UPPER, median).otherwise(col(c))
      )
    }

    // ── Пишем Parquet-витрину на общий том ─────────────────────────
    df.write.mode("overwrite").parquet(OUT_PATH)
    println(s"[DataMart] Saved to $OUT_PATH")

    spark.stop()
  }
}