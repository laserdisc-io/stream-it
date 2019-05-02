package streamit

import java.util.UUID

import cats.Show
import laserdisc.fs2.RedisAddress
import laserdisc.{ Host, Port }

sealed abstract class Settings {
  val kafka: Option[KafkaSettings]
  val api: Option[ApiSettings]
  val redis: Option[RedisSettings]
}

object Settings {

  @inline def apply(
    zks: Option[ZookeeperServer],
    cgi: Option[ConsumerGroupId],
    bts: Option[BootstrapServers],
    sry: Option[SchemaRegistryURL],
    abu: Option[APIBaseURL],
    rho: Option[Host],
    rpo: Option[Port],
  ): Settings =
    new Settings {
      override val kafka: Option[KafkaSettings] = KafkaSettings.from(cgi, zks, bts, sry)
      override val api: Option[ApiSettings]     = ApiSettings.from(abu)
      override val redis: Option[RedisSettings] = RedisSettings.from(rho, rpo)
    }

  implicit val show: Show[Settings] = (s: Settings) => s"""
                                                          | kafka: ${s.kafka}
                                                          | api: ${s.api}
                                                          | redis: ${s.redis}
      """.stripMargin
}

final case class ApiSettings(baseURL: APIBaseURL)

final case class RedisSettings(redisAddress: RedisAddress)

final case class KafkaSettings(
  groupId: ConsumerGroupId,
  zookeeper: Option[ZookeeperServer],
  bootstrapServers: BootstrapServers,
  schemaRegistry: SchemaRegistryURL
)

object ApiSettings {
  def from(baseURL: Option[APIBaseURL]): Option[ApiSettings] =
    baseURL.map(b => ApiSettings(b))
}

object KafkaSettings {
  def from(
    cgi: Option[ConsumerGroupId],
    zks: Option[ZookeeperServer],
    bts: Option[BootstrapServers],
    sr: Option[SchemaRegistryURL]
  ): Option[KafkaSettings] =
    for {
      c <- Some(cgi.getOrElse(s"stream-it-${UUID.randomUUID()}"))
      b <- bts
      s <- sr
    } yield KafkaSettings(c, zks, b, s)
}

object RedisSettings {
  def from(host: Option[Host], port: Option[Port]): Option[RedisSettings] =
    for {
      h <- host
      p <- port
    } yield RedisSettings(RedisAddress(h, p))
}
