package streamit.runner

import cats.effect.{ ConcurrentEffect, ContextShift, Timer }
import streamit.{ GeneralTask, Settings, Task }
import fs2.Stream
import log.effect.LogWriter

/**
  * The result of executing a task
  */
sealed trait Result extends Product with Serializable {

  /**
    * The task that wa performed to get this result
    */
  val task: Task
}
case class Success(override val task: Task)                  extends Result
case class Failure(override val task: Task, failure: String) extends Result

/**
  * An implementation of a task runner
  */
abstract class TaskRunner[F[_], +T] {
  def run[TT >: T <: Task](task: TT): Stream[F, Result]
}

object TaskRunner {
  def runnerOf[F[_]: ConcurrentEffect: ContextShift: Timer: LogWriter, T](
    settings: Settings,
    t: Task
  ): Stream[F, TaskRunner[F, Task]] =
    t match {
//      case _: RedisTask   => RedisTaskRunner[F](settings)
//      case _: KafkaTask   => KafkaTaskRunner[F](settings)
//      case _: ApiTask     => ApiTaskRunner[F](settings)
      case _: GeneralTask => GeneralTaskRunner[F]()
    }

}
