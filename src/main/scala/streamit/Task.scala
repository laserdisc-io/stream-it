package streamit

import scala.concurrent.duration.FiniteDuration

/**
  * base trait for all tasks
  */
sealed trait Task extends Product with Serializable {
  def desc: String
}

/**
  * trait for all tasks that aren't specific to something outside of this library
  */
sealed trait GeneralTask extends Task

/**
  * Sleep for [[duration]]
  */
final case class Sleep(duration: FiniteDuration) extends GeneralTask {
  override def desc: String = s"Sleep for $duration"
}
