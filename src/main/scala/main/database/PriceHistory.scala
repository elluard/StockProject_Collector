package main.database

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import main.collectors.Aggregates.Price
import slick.jdbc.GetResult

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object PriceHistory {
  implicit val system : ActorSystem = ActorSystem("PriceHistory")
  implicit val GetTickerRow: GetResult[TickerRow] = GetResult(r => TickerRow(r.nextString(), LocalDate.parse(r.nextString())) )
  implicit val getPriceHistory: GetResult[PriceHistoryRow] = GetResult(r => PriceHistoryRow(r.nextString(), r.nextFloat(), r.nextFloat(), r.nextFloat(), r.nextFloat(), r.nextInt(), r.nextFloat(), r.nextFloat(), r.nextString()))
  implicit val getNamePriceHistory: GetResult[NamePriceHistoryRow] = GetResult(r => NamePriceHistoryRow(r.nextString(), r.nextString(), r.nextFloat(), r.nextFloat(), r.nextFloat(), r.nextFloat(), r.nextInt(), r.nextFloat(), r.nextFloat(), r.nextString()))

  case class TickerRow(ticker : String, latestDate : LocalDate)
  case class RequestTicker(ticker : String, startDate : LocalDate, endDate: LocalDate)
  case class PriceHistoryRow(ticker: String, openPrice: Float, closePrice: Float, lowestPrice: Float, highestPrice: Float, transactionCount: Int, volumeWeightedAvg: Float, tradingVolume: Float, date: String)
  case class NamePriceHistoryRow(ticker: String, name: String, openPrice: Float, closePrice: Float, lowestPrice: Float, highestPrice: Float, transactionCount: Int, volumeWeightedAvg: Float, tradingVolume: Float, date: String)

  implicit val session = SlickSession.forConfig("slick-postgres")
  import session.profile.api._
  system.registerOnTermination(session.close())

  def selectTicker = {
    Slick
      .source(
        sql"""
            select tickers.ticker, coalesce(max(p.date), '2021-01-01') from tickers
              left join pricehistory p on tickers.ticker = p.ticker
            group by tickers.ticker;
         """.as[TickerRow])
  }

  def insertIntoPriceHistory = {
    Slick.sink{ row : (String, Price)=>
      sqlu"""insert into pricehistory (
                      "ticker", "closePrice", "highestPrice", "lowestPrice", "transactionCount", "openPrice", "tradingVolume", "volumeWeightedAvg",
                      "date"
                      )
              values (${row._1}, ${row._2.c}, ${row._2.h}, ${row._2.l}, ${row._2.n}, ${row._2.o}, ${row._2.v}, ${row._2.vw}, ${new Date(row._2.t)});"""
    }
  }

  def selectPriceHistory(ticker: String, start: String, end: String) = {
    val startDate = Date.valueOf(LocalDate.parse(start, DateTimeFormatter.ISO_DATE))
    val endDate = Date.valueOf(LocalDate.parse(end, DateTimeFormatter.ISO_DATE))
    Slick
      .source(
        sql"""
          select
             "ticker", "openPrice", "closePrice", "lowestPrice", "highestPrice",
             "transactionCount", "volumeWeightedAvg", "tradingVolume", "date"
          from pricehistory
          where
              ticker = $ticker and
              date >= $startDate and date <= $endDate
          order by date
       """.as[PriceHistoryRow]
      )
  }

  def selectTickers = {
    Slick.source(
      sql"""
        select "ticker" from pricehistory group by ticker
         """.as[String])
  }

  def selectLatest2DaysHistory(ticker: String) = {
    Slick.source(
      sql"""
          select
           A."ticker", t.name,"openPrice", "closePrice", "lowestPrice", "highestPrice",
           "transactionCount", "volumeWeightedAvg", "tradingVolume", "date"
          from pricehistory A
          join tickers t on A.ticker = t.ticker
          where A.ticker = $ticker
          order by date desc
          limit 2
         """.as[NamePriceHistoryRow]
    )
  }
}
