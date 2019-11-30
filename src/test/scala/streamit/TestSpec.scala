package streamit

import matchers.should.Matchers._
import streamit.runner.Result._

import scala.concurrent.duration._
import org.scalatest.matchers

class TestSpec extends StreamItSpec {

  val sleep100Millis = Sleep(100.millis)
  val sleep200Millis = Sleep(200.millis)
  val sleep300Millis = Sleep(300.millis)

  "StreamItSpec" should {

    "execute the provided tasks in order regardless of outcome" in {

      val spec   = Spec(Seq(sleep100Millis, sleep200Millis, sleep300Millis))
      val result = runSpecWithIO(spec)
      result.durationMs   should be > 0L
      result.results.size should equal(3)
      result.results(0)   should equal(Success(sleep100Millis))
      result.results(1)   should equal(Success(sleep200Millis))
      result.results(2)   should equal(Success(sleep300Millis))

    }

  }

}
