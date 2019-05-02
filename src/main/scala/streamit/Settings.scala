package streamit

import cats.Show

sealed abstract class Settings {
  // TODO: settings coming soon
}

object Settings {

  @inline def apply(): Settings =
    new Settings {
      // as we implement runner types, their settings will be grouped here
    }

  implicit val show: Show[Settings] = (s: Settings) => s"""
                                                          TODO: settings coming soon
      """.stripMargin
}
