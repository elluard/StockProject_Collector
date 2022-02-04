package main.collectors

import akka.Done
import akka.actor.{ActorLogging, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.stream.FlowShape
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source}
import com.typesafe.config.ConfigFactory
import slick.jdbc.GetResult
import spray.json._

import java.time.LocalDate
import java.sql.Date
import java.time.temporal.ChronoUnit
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object Aggregates {
  case class Price(c: Double, h: Double, l: Double, n: Int, o: Double, t: Long, v: Int, vw: Double)
  case class ResponseObject (adjusted: Boolean, queryCount: Int, request_id: String, results: Option[List[Price]], resultsCount: Int, status: String, ticker: String)
}

class HistoryCollector extends DefaultJsonProtocol {
  import Aggregates._

  case class TickerRow(ticker : String, latestDate : LocalDate)
  case class RequestTicker(ticker : String, startDate : LocalDate, endDate: LocalDate)

  implicit val system : ActorSystem = ActorSystem("HistoryCollector")
  implicit val GetTickerRow = GetResult( r => TickerRow(r.nextString(), LocalDate.parse(r.nextString())) )
  implicit val priceFormat: RootJsonFormat[Price] = jsonFormat8(Price)
  implicit val responseFormat: RootJsonFormat[ResponseObject] = jsonFormat7(ResponseObject)

  val loggerOutput: Sink[(Boolean, String), Future[Done]] = Sink.foreach[(Boolean, String)](x => println(s"Meaningful thing 2 : $x"))

  val httpRequestGraph = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    def successFilter(res : (Try[HttpResponse], Int)) = res match {
      case (Success(response), _) if response.status == StatusCodes.OK => true
      case _ => false
    }

    val pool = builder.add(Http().cachedHostConnectionPoolHttps[Int]("api.polygon.io")) //HttpRequest to Try[HttpResponse]
    val broadcast = builder.add(Broadcast[(Try[HttpResponse], Int)](2))
    val successFilterFlow = builder.add(
      Flow[(Try[HttpResponse], Int)]
        .filter(successFilter)
        .flatMapConcat(_._1.get.entity.dataBytes)
        .map(a => a.utf8String.parseJson.convertTo[ResponseObject])
        .map(a => (a.ticker, a.results.getOrElse(List())))
        .via(Flow[(String, List[Price])].flatMapConcat { a =>
          println(s"Ticker ${a._1}, data ${a._2}")
          Source(for {
            history <- a._2
          } yield (a._1, history))
        })
    )

    val logFlow = builder.add(
      Flow[(Try[HttpResponse], Int)]
        .map {
          case(Success(response), _) => (true, response.toString())
          case (Failure(ex), _) => (false, ex.toString)
        }
    )

    pool ~> broadcast ~> successFilterFlow
    broadcast ~> logFlow ~> loggerOutput

    FlowShape(pool.in, successFilterFlow.out)
  }

  def makeRequestTickerList(ticker : String, latestDate : LocalDate) = {
    val today = LocalDate.now()
    val before2Years = today.minusYears(2)
    val start = if (before2Years.isBefore(latestDate)) latestDate else before2Years

    val diffMonth = ChronoUnit.MONTHS.between(latestDate, LocalDate.now()).toInt
    val tickerList = (0 to diffMonth).toList
      .map{a =>
        RequestTicker(ticker, start.plusDays(1).plusMonths(a), start.plusMonths(a + 1))
      }

    Source(tickerList)
  }

  def makeHttpRequest(req: RequestTicker) : (HttpRequest, Int) = {
    val separateConfig = ConfigFactory.load("polygon-io.conf")
    (
      HttpRequest(
        HttpMethods.GET,
        uri = Uri(s"/v2/aggs/ticker/${req.ticker}/range/1/day/${req.startDate.plusDays(1).toString}/${req.endDate.toString}")
        .withQuery(Query("apiKey" -> separateConfig.getString("polygon-io.api-key")))
      ),
      0
    )
  }

  def insertIntoPriceHistory = {
    implicit val session = SlickSession.forConfig("slick-postgres")
    import session.profile.api._
    system.registerOnTermination(session.close())
    Slick.sink{ row : (String, Price)=>
      sqlu"""insert into pricehistory (
                      "ticker", "closePrice", "highestPrice", "lowestPrice", "transactionCount", "openPrice", "tradingVolume", "volumeWeightedAvg",
                      "date"
                      )
              values (${row._1}, ${row._2.c}, ${row._2.h}, ${row._2.l}, ${row._2.n}, ${row._2.o}, ${row._2.v}, ${row._2.vw}, ${new Date(row._2.t)});"""
    }
  }

  def selectTicker = {
    implicit val session = SlickSession.forConfig("slick-postgres")
    import session.profile.api._
    system.registerOnTermination(session.close())

    Slick
      .source(
        sql"""
            select tickers.ticker, coalesce(max(p.date), '2021-01-01') from tickers
              left join pricehistory p on tickers.ticker = p.ticker
            group by tickers.ticker;
         """.as[TickerRow])
  }

  // 주식 과거기록 조회, DB 에 있는 Ticker 의 가장 마지막 수집일 부터 당일까지 데이터 전부 읽어들임
  // polygon.io 제한있음, 1분에 request 5개.
  // 안전하게 1분에 4개씩 request 날리는것으로 해결
  def getAllHistory = {
    selectTicker
      .flatMapConcat(a => makeRequestTickerList(a.ticker, a.latestDate))
      .filter(a => a.endDate.isAfter(a.startDate))
      .map(makeHttpRequest)
      .throttle(4, 1 minute)
      .log("getAllHistory")
      .via(httpRequestGraph)
      .to(insertIntoPriceHistory)
      .run()
  }

  def getHistory(ticker : String, start : LocalDate, end : LocalDate) = {
    Source
      .single(makeHttpRequest(RequestTicker(ticker, start, end)))
      .via(httpRequestGraph)
      .to(insertIntoPriceHistory)
  }
}
