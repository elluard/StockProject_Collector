package main

import main.collectors.HttpOnly.HostLevelClient

object CollectorHostLevelClient extends App {
  val client = new HostLevelClient()
  client.getHistory
}
