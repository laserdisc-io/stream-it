package streamit

import java.util.concurrent.Executors

import cats.effect.{ ContextShift, IO, Timer }
import cats.implicits._
import log.effect.fs2.SyncLogWriter.log4sLog
import org.scalatest.WordSpec
import streamit.util.TestStats

import scala.concurrent.{ ExecutionContext, ExecutionContextExecutorService }

trait StreamItSpec extends WordSpec {

  implicit protected val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(
      Executors.newWorkStealingPool(Runtime.getRuntime.availableProcessors * 2)
    )

  implicit protected lazy val contextShift: ContextShift[IO] = IO.contextShift(ec)

  implicit protected lazy val timer: Timer[IO] = IO.timer(ec)

  def runSpecWithIO(spec: Spec): TestStats =
    log4sLog[IO]("stream-it")
      .flatMap { implicit logger =>
        logger.info("starting app") *>
          new SpecRunner[IO](Settings())
            .run(spec)
            .compile
            .toList
      }
      .unsafeRunSync()
      .head
}
