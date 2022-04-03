package main

import akka.actor.ActorSystem
import main.collectors.HttpOnly.RequestLevelClient

import scala.util.{Failure, Success}

object CollectorRequestLevelClient extends App {
  val client = new RequestLevelClient()
  client.getHistory
}
