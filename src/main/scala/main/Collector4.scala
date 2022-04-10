package main

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Sink, Source}
import main.collectors.UseStream.HistoryCollector
import slick.jdbc.GetResult

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

//case class TickerRow(idx: Int, ticker : String, name: String, ticker_type: String, locale: String)

//case class TickerRow(ticker : String, latestDate : LocalDate)

object Collector4 extends App {
  implicit val system = ActorSystem("ConnectionLevel")
  val collector = new HistoryCollector()
  collector.getHistory
}
