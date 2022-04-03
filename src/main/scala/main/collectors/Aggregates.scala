package main.collectors

object Aggregates {
  case class Price(c: Double, h: Double, l: Double, n: Int, o: Double, t: Long, v: Int, vw: Double)
  case class ResponseObject (adjusted: Boolean, queryCount: Int, request_id: String, results: Option[List[Price]], resultsCount: Int, status: String, ticker: String)
}
