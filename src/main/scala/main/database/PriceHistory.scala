package main.database

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import main.collectors.Aggregates.Price
import slick.jdbc.GetResult

import java.sql.Date
import java.time.LocalDate

object PriceHistory {
  implicit val system : ActorSystem = ActorSystem("PriceHistory")
  implicit val GetTickerRow: GetResult[TickerRow] = GetResult(r => TickerRow(r.nextString(), LocalDate.parse(r.nextString())) )

  case class TickerRow(ticker : String, latestDate : LocalDate)
  case class RequestTicker(ticker : String, startDate : LocalDate, endDate: LocalDate)
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
}
