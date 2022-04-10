package main.collectors.UseStream

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.ActorAttributes
import main.collectors.Aggregates
import main.common.HttpAkkaStream.httpRequestGraph
import main.common.HttpCommon.fromRequestTickerToHttpRequestTuple
import main.common.{HttpCommon, SuperVisor}
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

  // 주식 과거기록 조회, DB 에 있는 Ticker 의 가장 마지막 수집일 부터 당일까지 데이터 전부 읽어들임
  // polygon.io 제한있음, 1분에 request 5개.
  // 안전하게 1분에 4개씩 request 날리는것으로 해결
  def getHistory = {
    PriceHistory
      .selectTicker
      .flatMapConcat(a => HttpCommon.makeRequestTickerList(a.ticker, a.latestDate))
      .filter(a => a.endDate.isAfter(a.startDate))
      .map( fromRequestTickerToHttpRequestTuple )
      .throttle(4, 1 minute)
      .via( httpRequestGraph )
      .log("getAllHistory")
      .withAttributes(ActorAttributes.supervisionStrategy(SuperVisor.decider))
      .watchTermination() { (_, done) =>
        done.onComplete {
          case Success(_) => println("source completed successfully")
          case Failure(e) => println(s"source completed with failure : $e")
        }
      }
//      .to(PriceHistory.insertIntoPriceHistory)
      .runWith(Sink.foreach(println))
  }
}
