package streamit.runner

import cats.Show
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
}

object TaskRunner {
  def runnerOf[F[_]: ConcurrentEffect: ContextShift: Timer: LogWriter, T](
    settings: Settings,
    t: Task
  ): Stream[F, TaskRunner[F, Task]] =
    t match {
      // more specific runner types coming s
      case _: GeneralTask => GeneralTaskRunner[F]()
    }

}
