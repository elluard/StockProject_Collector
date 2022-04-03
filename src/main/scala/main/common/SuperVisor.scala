package main.common

import akka.stream.Supervision

object SuperVisor {
  val decider : Supervision.Decider = {
    case e : Exception  => {
      // logger 사용해서 날짜 찍을 수 있도록 처리해야 함
      println(e.toString)
      Supervision.Resume
    }
  }
}
