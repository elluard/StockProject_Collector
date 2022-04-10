package main.collectors.UseStream

import akka.actor.ActorSystem
import akka.stream.ActorAttributes
import main.collectors.Aggregates
import main.common.HttpAkkaStream.httpRequestGraph
import main.common.HttpCommon.{ fromRequestTickerToHttpRequestTuple, fromTickerRowToRequestTicker }
import main.common.SuperVisor
import main.database.PriceHistory
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class HistoryCollector extends DefaultJsonProtocol {
  import Aggregates._

  implicit val system : ActorSystem = ActorSystem("HistoryCollector")
  implicit val priceFormat: RootJsonFormat[Price] = jsonFormat8(Price)
  implicit val responseFormat: RootJsonFormat[ResponseObject] = jsonFormat7(ResponseObject)

  def getHistory = {
    PriceHistory
      .selectTicker //PriceHistory.TickerRow
      .flatMapConcat( fromTickerRowToRequestTicker ) // HttpComm.RequestTicker
      .map( fromRequestTickerToHttpRequestTuple ) // (HttpRequest, Int)
      .throttle(4, 1 minute)
      .via( httpRequestGraph )    // 여기서, 로그 기록 작업, output 으로는 (String, Price 리턴)
      .log("getAllHistory") //(String, Price)
      .withAttributes(ActorAttributes.supervisionStrategy(SuperVisor.decider))
      .watchTermination() { (_, done) =>
        done.onComplete {
          case Success(_) => println("source completed successfully")
          case Failure(e) => println(s"source completed with failure : $e")
        }
      }
      .to(PriceHistory.insertIntoPriceHistory)
      .run()
  }
}
