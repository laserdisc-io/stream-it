package streamit.runner

import cats.effect.{ ConcurrentEffect, ContextShift, Timer }
import fs2.Stream
import log.effect.LogWriter
import log.effect.fs2.syntax._
import streamit.{ GeneralTask, Sleep, Task }

object GeneralTaskRunner {
  def apply[F[_]: ConcurrentEffect: ContextShift: Timer: LogWriter](): Stream[F, GeneralTaskRunner[F]] =
    Stream(new GeneralTaskRunnerImpl)
}

sealed trait GeneralTaskRunner[F[_]] extends TaskRunner[F, GeneralTask]

class GeneralTaskRunnerImpl[F[_]: Timer](implicit logger: LogWriter[F])
    extends GeneralTaskRunner[F] {

  def runSleep(t: Sleep): Stream[F, Success] =
    logger.infoS(s"finished sleeping for $t").as(Success(t)).delayBy(t.duration)

  override def run[TT >: GeneralTask <: Task](task: TT): Stream[F, Result] =
    task match {
      case t: Sleep => runSleep(t)
    }
}
