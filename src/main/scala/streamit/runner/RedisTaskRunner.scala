package streamit.runner

import java.nio.channels.AsynchronousChannelGroup.withThreadPool
import java.util.concurrent.Executors.newFixedThreadPool

import cats.MonadError
import cats.effect._
import cats.syntax.flatMap._
import fs2.Stream
import laserdisc._
import laserdisc.fs2.{ RedisClient => LaserDiscClient, _ }
import log.effect.LogWriter
import streamit._
import streamit.syntax._
import streamit.runner.Result.{ Failure, Success }

object RedisTaskRunner {

  def apply[F[_]: ConcurrentEffect: ContextShift: Timer: LogWriter](
    settings: F[Settings]
  ): Stream[F, RedisTaskRunner[F]] =
    Stream
      .eval(settings)
      .map(_.redis)
      .flatMap {
        case None =>
          Stream.apply(new RedisNoOpTaskRunner())

        case Some(s) =>
          val acgResource =
            MkResource(ConcurrentEffect[F].delay(withThreadPool(newFixedThreadPool(2))))
          Stream.resource(acgResource).flatMap { implicit acg =>
            LaserDiscClient[F](Set(s.redisAddress)).map(new RedisTaskRunnerImpl(_))
          }
      }
}

sealed trait RedisTaskRunner[F[_]] extends TaskRunner[F, RedisTask]

// TODO: extract common NoOp runner and eliminate this
final class RedisNoOpTaskRunner[F[_]] extends RedisTaskRunner[F] {
  def run[TT >: RedisTask <: Task](task: TT): Stream[F, Result] =
    fail(task)
}

class RedisTaskRunnerImpl[F[_]](client: LaserDiscClient[F])(
  implicit F: MonadError[F, Throwable],
  logger: LogWriter[F]
) extends RedisTaskRunner[F] {

  def run[TT >: RedisTask <: Task](task: TT): Stream[F, Result] =
    task match {
      case t: RedisGetAndVerify => runGetAndVerify(t)
    }

  def runGetAndVerify(t: RedisGetAndVerify): Stream[F, Result] =
    redisGetValue(Key.unsafeFrom(t.key))
      .map(v => v.asJson)
      .collectFirst {
        case jsonRec if jsonRec == t.expected => Success(t)
        case jsonRec =>
          Failure(t, s"Failed match for key ${t.key}. Expected '${t.expected}', got '$jsonRec'")
      }
      .handleErrorWith(
        e => Stream(Failure(t, s"${e.getClass.getName}: ${e.getMessage}"))
      )

  def redisGetValue(redisKey: Key): Stream[F, String] =
    Stream.eval(client.send1(strings.get[String](redisKey)).flatMap {

      case Right(Some(value)) =>
        logger.info(s"For key $redisKey got value: $value") >> F.pure(value)

      case Right(None) =>
        F.raiseError[String](new RuntimeException(s"No value found for key: $redisKey"))

      case Left(e) =>
        F.raiseError[String](new RuntimeException(s"Error loading value for key: $redisKey", e))

    })
}
