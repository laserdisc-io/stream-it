package streamit.runner

import cats.Show
import cats.effect.{ ConcurrentEffect, ContextShift, Timer }
import fs2.Stream
import log.effect.LogWriter
import streamit._
import streamit.runner.Result.Failure

/**
  * The result of executing a task
  */
sealed trait Result extends Product with Serializable {

  /**
    * The task that wa performed to get this result
    */
  val task: Task
}

object Result {
  final case class Success(override val task: Task)                  extends Result
  final case class Failure(override val task: Task, failure: String) extends Result

  implicit val showResult: Show[Result] = (result: Result) => {
    val taskName = result.task.getClass.getSimpleName
    result match {
      case Success(t)      => s"""👍 Success: $taskName[${t.desc}]"""
      case Failure(t, err) => s"""❌ Failure: $taskName[${t.desc} -> $err ]"""
    }
  }
}

/**
  * An implementation of a task runner
  */
abstract class TaskRunner[F[_], +T] {
  def run[TT >: T <: Task](task: TT): Stream[F, Result]

  def fail[TT >: T <: Task](task: TT) =
    Stream(
      Failure(
        task,
        s"${getClass.getName} can't run action '${task.desc}', runner is not implemented!"
      )
    )
}

object TaskRunner {
  def runnerOf[F[_]: ConcurrentEffect: ContextShift: Timer: LogWriter, T](
    settings: F[Settings],
    t: Task
  ): Stream[F, TaskRunner[F, Task]] =
    t match {
      case _: RedisTask   => RedisTaskRunner[F](settings)
      case _: KafkaTask   => KafkaTaskRunner[F](settings)
      case _: ApiTask     => ApiTaskRunner[F](settings)
      case _: GeneralTask => GeneralTaskRunner[F]()
    }

}
