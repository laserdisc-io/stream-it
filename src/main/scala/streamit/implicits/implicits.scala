package streamit

import cats.Show
import streamit.runner.{ Failure, Result, Success }

import scala.language.implicitConversions

package object implicits {

  implicit val showResult: Show[Result] = (result: Result) => {
    val taskName = result.task.getClass.getSimpleName
    result match {
      case Success(t)      => s"""👍 Success: $taskName[${t.desc}]"""
      case Failure(t, err) => s"""❌ Failure: $taskName[${t.desc} -> $err ]"""
    }
  }
}
