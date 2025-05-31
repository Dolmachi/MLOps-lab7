import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.Source
import akka.util.ByteString
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import scala.jdk.CollectionConverters._
import akka.NotUsed

case class Prediction(_id: String, cluster: Int)

object DataMartServer {

  implicit val system = ActorSystem("DataMartServer")
  implicit val ec     = system.dispatcher
  private val logger  = LogManager.getLogger(getClass)

  /* ───── helpers ─────────────────────────────────────────────── */

  /** Потоково превращает DataFrame → JSON-массив `[row1,row2,…]` */
  private def jsonStream(df: DataFrame): Source[ByteString, NotUsed] =
    Source
      .fromIterator(() => df.toJSON.toLocalIterator().asScala)
      .map(ByteString(_))
      .intersperse(ByteString("["), ByteString(","), ByteString("]"))

  /** Берём «окно» данных без collect() */
  private def processedSlice(offset: Long, limit: Int): DataFrame = {
    val all = DataMart.preprocessData(DataMart.getRawData)
    all
      .withColumn("rn", monotonically_increasing_id())
      .filter(col("rn") >= offset)
      .limit(limit)
      .drop("rn")
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
          parameters("offset".as[Long].?(0L), "limit".as[Int].?(10000)) { (offset, limit) =>
            get {
              val slice  = processedSlice(offset, limit)
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
        DataMart.stop(); system.terminate()
      }
    }
  }
}