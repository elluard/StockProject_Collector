package main

import main.collectors.HttpOnly.ConnectionLevelClient

object CollectorConnectionLevelClient extends App {
  val client = new ConnectionLevelClient();
  client.getHistory
}
