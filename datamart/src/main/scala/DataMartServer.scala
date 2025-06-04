import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, spark_partition_id}
import org.apache.spark.storage.StorageLevel
import scala.jdk.CollectionConverters._
import akka.NotUsed

case class Prediction(_id: String, cluster: Int)

object DataMartServer {

  implicit val system = ActorSystem(
    "DataMartServer",
    ConfigFactory
      .parseString("akka.http.server.request-timeout = 600s")
      .withFallback(ConfigFactory.load())
  )
  implicit val ec     = system.dispatcher
  private val logger  = LogManager.getLogger(getClass)

  /* ───── helpers ─────────────────────────────────────────────── */

  private val numPartitions = 30 // Разбиваем на 30 партиций (~100 000 строк в каждой)

  private val processedAll: DataFrame = {
    val base = DataMart.preprocessData(DataMart.getRawData)
      .repartition(numPartitions) // Разбиваем на 30 партиций
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"⇢ Кэшированный датасет, кол-во записей = ${base.count()}")
    base
  }

  private val processedAllWithPartitions = processedAll
    .withColumn("partitionIndex", spark_partition_id()) // Добавляем столбец с номером партиции

  private def jsonStream(df: DataFrame): Source[ByteString, NotUsed] =
    Source
      .fromIterator(() => df.toJSON.toLocalIterator().asScala)
      .map(ByteString(_))
      .intersperse(ByteString("["), ByteString(","), ByteString("]"))

  private def processedSlice(partitionIndex: Int): DataFrame = {
    processedAllWithPartitions
      .where(col("partitionIndex") === partitionIndex)
      .drop("partitionIndex") // Убираем временный столбец
  }

  /* ───── routes ──────────────────────────────────────────────── */

  val route =
    pathPrefix("api") {
      concat(
        /* health-check */
        path("health") {
          get { complete("""{"status":"OK"}""") }
        },

        /* предобработанные данные */
        path("processed-data") {
          parameters("partitionIndex".as[Int]) { partitionIndex =>
            get {
              val slice = processedSlice(partitionIndex)
              val entity = HttpEntity.Chunked.fromData(
                ContentTypes.`application/json`,
                jsonStream(slice)
              )
              complete(entity)
            }
          }
        },

        /* приём предсказаний */
        path("predictions") {
          post {
            entity(as[List[Prediction]]) { preds =>
              val spark = DataMart.spark; import spark.implicits._
              DataMart.savePredictions(preds.toDF())
              complete(StatusCodes.OK, "Предсказания успешно сохранены")
            }
          }
        }
      )
    }

  /* ───── main ───────────────────────────────────────────────── */

  def main(args: Array[String]): Unit = {
    val binding = Http().newServerAt("0.0.0.0", 8080).bind(route)
    println("⇢ DataMart-API запущен: http://0.0.0.0:8080/api")

    sys.addShutdownHook {
      binding.flatMap(_.unbind()).onComplete { _ =>
        processedAll.unpersist()
        DataMart.stop()
        system.terminate()
      }
    }
  }
}