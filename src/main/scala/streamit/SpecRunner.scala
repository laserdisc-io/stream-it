package streamit

import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.show._
import cats.syntax.traverse._
import fs2.Stream
import log.effect.LogWriter
import streamit.runner.Result._
import streamit.runner.TaskRunner
import streamit.util.SpecTimer._
import streamit.util.TestStats

/**
  * A Spec (specification) is the sequence of [[Task]]s to perform, be they actions that trigger
  * testable behaviour in the system, or the actual verifications themselves.
  */
case class Spec(tasks: Seq[Task]) {
  def logPreview[F[_]: ConcurrentEffect](implicit logger: LogWriter[F]): F[Unit] =
    logger.info(s" =============== spec preview  =============== ") *>
      logger.info(s" tasks to perform: ") *>
      tasks.map(t => logger.info(s"  - ${t.getClass.getSimpleName}: ${t.desc}")).toList.sequence *>
      logger.info(s" ============================================= ")
}

final class SpecRunner[F[_]: ConcurrentEffect: ContextShift: Timer](settings: Settings)(
  implicit logger: LogWriter[F]
) {

  def run(spec: Spec): Stream[F, TestStats] =
    Stream(spec)
      .evalTap { _.logPreview }
      .flatMap { s =>
        def logSummary(stats: TestStats): Stream[F, TestStats] =
          Stream.eval {
            logger.info(s" =============== results  =============== ") *>
              stats.results.map(s => logger.info(s"  - ${s.show}")).sequence *>
              logger.info(
                s"       ${stats.total} tests: ${stats.passed.size} passed, ${stats.failed.size} failed"
              ) *>
              logger.info(
                s" =================== (duration: ${stats.durationMs}ms) ================== "
              ) *>
              ConcurrentEffect[F].pure(stats)
          }

        val runTasks = Stream(s.tasks: _*)
          .evalTap(t => logger.info(s" --- Running Task: $t"))
          .flatMap(t => TaskRunner.runnerOf(settings, t) >>= (_.run(t)))
          .compile
          .toList

        Stream
          .eval(runTimed(runTasks))
          // TODO: push success metric
          .flatMap(logSummary)
      }
  // TODO .handleErrorWith(push-metrics-then-rethrow)

}
