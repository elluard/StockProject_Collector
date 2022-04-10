package main.collectors.HttpOnly

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorAttributes
import main.common.HttpCommon.{fromHttpResponseToReponseBody, fromTickerRowToRequestTicker, httpRequestExceptionToResponse, makeHttpRequest, priceTupleFlow}
import main.database.PriceHistory
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

class RequestLevelClient extends DefaultJsonProtocol {
  import main.common._
  implicit val system : ActorSystem = ActorSystem("HistoryCollector_RequestLevel")

  def getHistory = {
    // 순서
    // 1. alpakka 로 record 를 읽어온다.
    // 2. HttpRequest 의 형태로 변환한다.
    // 3. 1분에 4개씩 진행하도록 한다.
    // 4. HttpRequest 를 전송한다.
    // 5. 결과를 확인한다.
    // 6. JSON 으로 변환한다.

    //확인할 수 있는것
    //1. if 문 을 사용한 에러처리가 없음
    //2. try ~ catch 문이 없음
    //3. 암묵적 형변환 가능

    //구현 시 특징
    // 1. 처리 절차 보다는 결과물에 집중하게 됨.
    //   반복문으로 처리 후, if 로 예외처리 같은 생각보다, Data type 에 대해 더 집중

    //느끼게 된 점
    //1. 처리 절차보다 데이터의 변환에 신경을 쓰게 됨.
    //2. 함수형 패러다임이 소스코드의 극적인 간결함을 보장해주진 않음.
    //3. 구현작업 후, 문서화는 여전히 필요하다.
    //4. 패러다임이 모든걸 해결해주진 않는다. -> 치명적인 상황에 빠지지 않게 도와줄 뿐, 결국 개발자가 모두 해 내야한다.
    PriceHistory
      .selectTicker // DB로부터 데이터 읽어들임
      .flatMapConcat( fromTickerRowToRequestTicker ) //데이터 가공
      .map( makeHttpRequest("https://api.polygon.io/v2/aggs/ticker/", _) )
      .throttle(4, 1 minute)  //여기서 1분에 4건 다음 step 으로 전진하게끔 한다.
      .via(Http().superPool())  //여기서 HTTP Request 전송, HttpReseponse 형태로 바뀐다.
      .map( httpRequestExceptionToResponse  )
      .flatMapConcat( fromHttpResponseToReponseBody )
      .via( priceTupleFlow )
      .withAttributes(ActorAttributes.supervisionStrategy(SuperVisor.decider)) //실패 시 복구 전략 및 exception handler
      .watchTermination() { (_, done) => //모든 작업이 끝난 후 혹은 처리되지 않은 exception 발생 시 호출
        done.onComplete {
          case Success(_) => println("source completed successfully")
          case Failure(e) => println(s"source completed with failure : $e")
        }
      }
      .to(PriceHistory.insertIntoPriceHistory)
      .run()
  }
}
