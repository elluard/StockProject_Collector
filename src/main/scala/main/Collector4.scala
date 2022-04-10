package main

import akka.actor.ActorSystem
import main.collectors.UseStream.HistoryCollector
import scala.language.postfixOps

object Collector4 extends App {
  implicit val system = ActorSystem("ConnectionLevel")
  val collector = new HistoryCollector()
  collector.getHistory
}
