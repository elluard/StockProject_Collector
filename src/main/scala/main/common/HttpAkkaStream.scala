package main.common

import akka.actor.ActorSystem
import akka.{Done, NotUsed}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink}
import main.collectors.Aggregates.Price
import main.common.HttpCommon.{fromHttpResponseToReponseBody, httpRequestExceptionToResponse, priceTupleFlow, requestSuccessFilter}
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object HttpAkkaStream extends  DefaultJsonProtocol {
  implicit val system : ActorSystem = ActorSystem("HttpAkkaStream")

  val httpRequestGraph: Graph[FlowShape[(HttpRequest, Int), (String, Price)], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val loggerOutput: Sink[(Boolean, String), Future[Done]] = Sink.foreach[(Boolean, String)](x => println(s"Collect Result : $x"))

    val pool = builder.add(Http().cachedHostConnectionPoolHttps[Int]("api.polygon.io")) //HttpRequest to Try[HttpResponse]
    val broadcast = builder.add(Broadcast[(Try[HttpResponse], Int)](2))
    val successFilterFlow = builder.add(
      Flow[(Try[HttpResponse], Int)]
        .map( httpRequestExceptionToResponse )
        .filter( requestSuccessFilter )
        .flatMapConcat( fromHttpResponseToReponseBody )
        .via( priceTupleFlow )
    )

    val logFlow = builder.add(
      Flow[(Try[HttpResponse], Int)]
        .map {
          case(Success(response), _) => (true, response.toString())
          case (Failure(ex), _) => (false, ex.toString)
        }
    )

    pool ~> broadcast ~> successFilterFlow
    broadcast ~> logFlow ~> loggerOutput

    FlowShape(pool.in, successFilterFlow.out)
  }
}
