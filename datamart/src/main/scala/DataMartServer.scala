import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import org.apache.spark.sql.DataFrame
import scala.io.StdIn

case class Prediction(_id: String, cluster: Int)

object DataMartServer {
  implicit val system = ActorSystem("DataMartServer")
  implicit val executionContext = system.dispatcher

  // Получение предобработанных данных
  def getProcessedData: DataFrame = {
    val rawData = DataMart.getRawData
    DataMart.preprocessData(rawData)
  }

  // Маршруты API
  val route =
    pathPrefix("api") {
      path("processed-data") {
        get {
          // Возвращаем предобработанные данные в формате JSON
          val df = getProcessedData
          val json = df.toJSON.collect().mkString("[", ",", "]")
          complete(HttpEntity(ContentTypes.`application/json`, json))
        }
      } ~
      path("predictions") {
        post {
          // Принимаем предсказания от модели
          entity(as[List[Prediction]]) { predictions =>
            val spark = DataMart.spark
            import spark.implicits._
            val predictionsDF = predictions.toDF()
            DataMart.savePredictions(predictionsDF)
            complete(StatusCodes.OK, "Предсказания успешно сохранены")
          }
        }
      }
    }

  def main(args: Array[String]): Unit = {
    val bindingFuture = Http().newServerAt("0.0.0.0", 8080).bind(route)

    println("Сервер запущен на http://0.0.0.0:8080/api")
    println("Нажмите Enter для завершения...")
    StdIn.readLine()

    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => {
        DataMart.stop()
        system.terminate()
      })
  }
}