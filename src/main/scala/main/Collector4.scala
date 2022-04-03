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
//  implicit val GetTickerRow = GetResult( r => TickerRow(r.nextInt(), r.nextString(), r.nextString(), r.nextString(), r.nextString()) )

//  val input = Source(1 to 100)
//  val fastSource = input.throttle(5, 1 minute)
//  val sink1 = Sink.foreach[Int](x => println(s"Sink1 from fastSource : $x"))
//  fastSource.runWith(sink1)
//  implicit val session = SlickSession.forConfig("slick-postgres")
//  system.registerOnTermination(session.close())
//  import session.profile.api._
//
  val collector = new HistoryCollector()
  val result = collector.getAllHistory

//  val test = LocalDate.of(2020, 1, 1).toEpochDay.until(LocalDate.now().toEpochDay).map(LocalDate.ofEpochDay)
//  println(test)

//  val test = ChronoUnit.MONTHS.between(LocalDate.of(2021,1,1), LocalDate.now()).toInt
//    LocalDate
//    .of(2021, 1, 1)
//    .compareTo( LocalDate.now())
//  val start = LocalDate.of(2021,1,1)
//  println(test)
//  println(
//    (0 to test).toList
//      .map(a => (start.plusMonths(a), start.plusMonths(a + 1)))
//  )

//  val test = collector.makeRequestTickerList("SPY", LocalDate.of(2022,2,1))
//  println(test)



  import system.dispatcher
//  result.onComplete {
//    case Success(value) => println(s"success $value")
//    case Failure(ex) => println(s"failure $ex")
//  }
}
