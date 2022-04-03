package main.collectors.UseStream

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source}
import akka.stream.{ActorAttributes, FlowShape, Graph, Supervision}
import akka.{Done, NotUsed}
import com.typesafe.config.ConfigFactory
import main.collectors.Aggregates
import main.database.PriceHistory.{RequestTicker, TickerRow}
import spray.json._

import java.sql.Date
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class HistoryCollector extends DefaultJsonProtocol {
  import Aggregates._

  implicit val system : ActorSystem = ActorSystem("HistoryCollector")
  implicit val priceFormat: RootJsonFormat[Price] = jsonFormat8(Price)
  implicit val responseFormat: RootJsonFormat[ResponseObject] = jsonFormat7(ResponseObject)

  val loggerOutput: Sink[(Boolean, String), Future[Done]] = Sink.foreach[(Boolean, String)](x => println(s"Meaningful thing 2 : $x"))

  val httpRequestGraph: Graph[FlowShape[(HttpRequest, Int), (String, Price)], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    def successFilter(res : (Try[HttpResponse], Int)) = res match {
      case (Success(response), _) if response.status == StatusCodes.OK => true
      case _ => false
    }

    def getJSONPayload(result : (Try[HttpResponse], Int)) = {
      result._1.get.entity.dataBytes
        .map(_.utf8String.parseJson.convertTo[ResponseObject])
    }

    val pool = builder.add(Http().cachedHostConnectionPoolHttps[Int]("api.polygon.io")) //HttpRequest to Try[HttpResponse]
    val broadcast = builder.add(Broadcast[(Try[HttpResponse], Int)](2))
    val successFilterFlow = builder.add(
      Flow[(Try[HttpResponse], Int)]
        .filter(successFilter)
        .flatMapConcat(getJSONPayload)
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

  // 에러처리를 위한 Supervision Strategy

  val decider : Supervision.Decider = {
    case e : Exception  => {
      // logger 사용해서 날짜 찍을 수 있도록 처리해야 함
      Supervision.Resume
    }
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
      .via(httpRequestGraph)
      .log("getAllHistory")
      .watchTermination() { (_, done) =>
        done.onComplete {
          case Success(_) => println("source completed successfully")
          case Failure(e) => println(s"source completed with failure : $e")
        }
      }
      .to(insertIntoPriceHistory)
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
      .run()
  }

  def getHistory(ticker : String, start : LocalDate, end : LocalDate) = {
    Source
      .single(makeHttpRequest(RequestTicker(ticker, start, end)))
      .via(httpRequestGraph)
      .to(insertIntoPriceHistory)
  }
}
