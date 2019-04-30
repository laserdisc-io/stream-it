package streamit

import org.scalatest.Matchers._
import org.scalatest.WordSpec

class HelloSpec extends WordSpec {

  "HelloApp" should {

    // placeholder
    "fart()" in {

      Hello.fart() should equal("\uD83D\uDCA8")

    }

  }

}
