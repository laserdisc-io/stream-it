package streamit.util

import cats.effect.{ ConcurrentEffect, ContextShift, Timer }
import cats.implicits._
import streamit.runner.{ Failure, Result, Success }

case class TestStats(durationMs: Long, results: List[Result]) {
  val passed: List[Success] = results.collect { case r: Success => r }
  val failed: List[Failure] = results.collect { case r: Failure => r }
  val total: Int            = results.size
}

object SpecTimer {

  /**
    * Attempt the given effect which will produce the task results, and wrap in a convenience
    * object containing the time taken to execute the effect
    */
  def runTimed[F[_]: ConcurrentEffect: ContextShift: Timer](fa: F[List[Result]]): F[TestStats] =
    ConcurrentEffect[F].delay(System.currentTimeMillis()).flatMap { startMs =>
      fa.attempt.flatMap {
        case Right(result) =>
          ConcurrentEffect[F].pure(TestStats(System.currentTimeMillis() - startMs, result))
        case Left(e) =>
          ConcurrentEffect[F].raiseError(e)
      }
    }

}
