package streamit.runner

import cats.effect._
import cats.syntax.flatMap._
import fs2.Stream
import io.circe.Json
import log.effect.LogWriter
import org.http4s.Status.{ NotFound, Successful }
import org.http4s.circe._
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.{ Header, Request, Uri }
import streamit._
import streamit.runner.Result.{ Failure, Success }

import scala.concurrent.ExecutionContext.global

object ApiTaskRunner {

  def apply[F[_]: ConcurrentEffect: ContextShift: Timer: LogWriter](
    settings: F[Settings]
  ): Stream[F, ApiTaskRunner[F]] =
    Stream
      .eval(settings)
      .map(_.api)
      .flatMap {
        case None    => Stream.apply(new NoOpApiTaskRunnerImpl())
        case Some(s) => Stream.apply(new ApiTaskRunnerImpl(s))
      }

}

sealed trait ApiTaskRunner[F[_]] extends TaskRunner[F, ApiTask]

// TODO: extract common NoOp runner and eliminate this
final class NoOpApiTaskRunnerImpl[F[_]] extends ApiTaskRunner[F] {
  def run[TT >: ApiTask <: Task](task: TT): Stream[F, Result] = fail(task)
}

class ApiTaskRunnerImpl[F[_]](settings: ApiSettings)(
  implicit F: ConcurrentEffect[F],
  logger: LogWriter[F]
) extends ApiTaskRunner[F] {

  def run[TT >: ApiTask <: Task](task: TT): Stream[F, Result] =
    task match {
      case t: ApiGETAndVerify => runGetAndVerify(t)
    }

  def runGetAndVerify(t: ApiGETAndVerify): Stream[F, Result] = {

    val path = s"${settings.baseURL}${t.uri}"

    performGET(path, t.headers)
      .collectFirst {
        case jsonRec if jsonRec == t.expected => Success(t)
        case jsonRec =>
          Failure(t, s"Failed match GET on $path. Expected '${t.expected}', got '$jsonRec'")
      }
      .handleErrorWith(
        e => Stream(Failure(t, s"${e.getClass.getName}: ${e.getMessage}"))
      )
  }

  def performGET(path: String, headers: Map[String, String]): Stream[F, Json] =
    Stream.eval(BlazeClientBuilder[F](global).resource.use(client => {

      val request = Request[F](
        uri = Uri.unsafeFromString(path)
      ).withHeaders((headers map {
        case (k, v) => Header(k, v).asInstanceOf[Header]
      }).toList: _*)

      logger.info(s"Performing GET on '$path'") >>
        client.fetch[Json](request) {
          case Successful(resp) => resp.decodeJson[Json]
          case NotFound(_)      => F.raiseError(new Exception(s" response for $path"))
          case resp =>
            resp.bodyAsText
              .flatMap(body => {
                Stream.raiseError(
                  new Exception(s"Unexected response for $path: $resp - body: $body")
                )
              })
              .as(Json.Null)
              .compile
              .lastOrError
        }

    }))
}
