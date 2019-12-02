package streamit

import java.util.concurrent.Executors

import cats.effect.{ ContextShift, IO, Timer }
import cats.implicits._
import log.effect.fs2.SyncLogWriter.log4sLog
import org.scalatest.wordspec.AnyWordSpec
import streamit.util.TestStats

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService }

trait StreamItSpec extends AnyWordSpec {

  implicit protected val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(
      Executors.newWorkStealingPool(Runtime.getRuntime.availableProcessors * 2)
    )

  implicit protected lazy val contextShift: ContextShift[IO] = IO.contextShift(ec)

  implicit protected lazy val timer: Timer[IO] = IO.timer(ec)

  def runSpecWithIO(spec: Spec): TestStats = {

    // TODO: we'll flesh optionals out as we add coverage for runners
    val settings = IO.pure(Settings(None, None, None, None, None, None, None))

    log4sLog[IO]("stream-it")
      .flatMap { implicit logger =>
        logger.info("starting app") >>
          new SpecRunner[IO](settings)
            .run(spec)
            .compile
            .toList
      }
      .unsafeRunSync()
      .head
  }
}
