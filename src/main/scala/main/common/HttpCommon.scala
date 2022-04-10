package main.common

import akka.actor.ActorSystem
import akka.{Done, NotUsed}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.http.scaladsl.model.Uri.Query
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source}
import com.typesafe.config.ConfigFactory
import main.collectors.Aggregates.{Price, ResponseObject}
import main.database.PriceHistory.TickerRow
import spray.json.DefaultJsonProtocol
import spray.json._

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object HttpCommon extends DefaultJsonProtocol {
  implicit val priceFormat: RootJsonFormat[Price] = jsonFormat8(Price)
  implicit val responseFormat: RootJsonFormat[ResponseObject] = jsonFormat7(ResponseObject)

  case class RequestTicker(ticker : String, startDate : LocalDate, endDate: LocalDate)

  def makeRequestTickerList(ticker : String, latestDate : LocalDate) = {
    //접수일로부터 최대 2년간, 1달씩 끊어 RequestTicker List 를 구성한다.
    val today = LocalDate.now()
    val before2Years = today.minusYears(2)
    val start = if (before2Years.isBefore(latestDate)) latestDate else before2Years

    val diffMonth = ChronoUnit.MONTHS.between(latestDate, LocalDate.now()).toInt
    val tickerList = (0 to diffMonth).toList
      .map{ a =>
        RequestTicker(ticker, start.plusDays(1).plusMonths(a), start.plusMonths(a + 1))
      }
      .filter(a => a.endDate.isAfter(a.startDate))

    Source(tickerList)
  }

  def fromTickerRowToRequestTicker(tickerRow : TickerRow) = {
    makeRequestTickerList(tickerRow.ticker, tickerRow.latestDate)
  }

  def makeHttpRequest(uri: String, req: RequestTicker) : (HttpRequest, Int) = {
    val separateConfig = ConfigFactory.load("polygon-io.conf")
    (
      HttpRequest(
        HttpMethods.GET,
        uri = Uri(s"$uri${req.ticker}/range/1/day/${req.startDate.plusDays(1).toString}/${req.endDate.toString}")
          .withQuery(Query("apiKey" -> separateConfig.getString("polygon-io.api-key")))
      ),
      0
    )
  }

  def fromRequestTickerToHttpRequest(req: RequestTicker) : HttpRequest = {
    val separateConfig = ConfigFactory.load("polygon-io.conf")
    HttpRequest(
      HttpMethods.GET,
      uri = Uri(s"/v2/aggs/ticker/${req.ticker}/range/1/day/${req.startDate.plusDays(1).toString}/${req.endDate.toString}")
        .withQuery(Query("apiKey" -> separateConfig.getString("polygon-io.api-key")))
    )
  }

  def fromRequestTickerToHttpRequestTuple(req: RequestTicker) : (HttpRequest, Int) =  ( fromRequestTickerToHttpRequest(req), 0 )

  def fromHttpResponseToReponseBody(httpResponse : HttpResponse) = {
    httpResponse.entity.dataBytes
      .map(_.utf8String.parseJson.convertTo[ResponseObject])
      .map(a => (a.ticker, a.results.getOrElse(List())))
  }

  def priceTupleFlowMap = Flow[(String, List[Price])].map { a =>
    println(s"Ticker ${a._1}, data ${a._2}")
    for {
      history <- a._2
    } yield (a._1, history)
  }

  def priceTupleFlow = Flow[(String, List[Price])].flatMapConcat { a =>
    Source(for {
      history <- a._2
    } yield (a._1, history))
  }

  def httpRequestExceptionToResponse(param : (Try[HttpResponse], Int)) = {
    param._1.getOrElse {
      //예외 발생 시, 아래 코드가 실행됨, 500 에러로 변환하여 저장한다.
      println(s"Error Occur, ${param.toString()}")
      HttpResponse(status = StatusCodes.InternalServerError)
    }
  }
}
