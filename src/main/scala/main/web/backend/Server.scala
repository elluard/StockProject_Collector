package main.web.backend

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import main.database.PriceHistory.{NamePriceHistoryRow, PriceHistoryRow, selectLatest2DaysHistory}
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.scaladsl.Sink
import main.common.HttpCommon.terminationWatchter
import main.database.PriceHistory
import spray.json.DefaultJsonProtocol._

object Server extends App {
  implicit val system = ActorSystem("Server")
  import akka.http.scaladsl.server.Directives._

  //implicit 특성에 의해 자동으로 json 변환이 일어난다.
  implicit val priceHistoryFormat = jsonFormat9(PriceHistoryRow)
  implicit val namePriceHistoryFormat = jsonFormat10(NamePriceHistoryRow)
  implicit val latestHistoryFormat = jsonFormat7(GetLatestDatHistory)

  case class GetPriceHistory(ticker: String, start: String, end: String)
  case class GetRecentHistory(ticker: String)
  case class GetLatestDatHistory(ticker: String, fullName: String,  date: String, closePrice: Float, transactionCount: Int, change: Float, tradingVolume: Float)

  def getPriceHistory(ticker: String, start: String, end: String) = {
    PriceHistory
      .selectPriceHistory(ticker, start, end)
      .watchTermination()(terminationWatchter)
      .runFold(List[PriceHistoryRow]())((a, b) => a ::: List(b) )
  }

  def getAllTickerRecentlyHistory() = {
    PriceHistory
      .selectTickers
      .flatMapConcat(getLatestDayHistory)
      .runFold(List[GetLatestDatHistory]())((a, b) => a ::: List(b) )
  }

  def getLatestDayHistory(ticker: String) = {
    PriceHistory
      .selectLatest2DaysHistory(ticker)
      .map { a =>
        GetLatestDatHistory(a.ticker, a.name, a.date, a.closePrice, a.transactionCount, a.closePrice, a.tradingVolume)
      }
      .reduce((a, b) => {
        GetLatestDatHistory(a.ticker, a.fullName, a.date, a.closePrice, a.transactionCount, a.closePrice - b.closePrice, a.tradingVolume)
      })
  }

  val getRoute =
    path("total") {
      get {
        complete(StatusCodes.OK, getAllTickerRecentlyHistory())
      }
    } ~
    path("stocks"){
      get {
        parameters("ticker".as[String], "start".as[String], "end".as[String]).as(GetPriceHistory) { params =>
          println(s"start : ${params.start}, end: ${params.end}")
          complete(StatusCodes.OK, getPriceHistory(params.ticker, params.start, params.end))
        }
      }
    }

  Http().newServerAt("localhost", 8080).bind(getRoute)
}
