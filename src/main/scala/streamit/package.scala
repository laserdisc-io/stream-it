import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.W

package object streamit {

  // a prototype of this app used k8s, which tended to enforce limitations on app names
  // we don't really need this in this library, but let's start out more restrictive and
  // remove this (letting the caller be restrictive), rather than the other way round..
  private[this] type AlphaNumDashes = String Refined MatchesRegex[W.`"^[a-zA-Z0-9\\\\-]+$"`.T]

  // settings types
  final type ZookeeperServer   = String
  final type BootstrapServers  = String
  final type SchemaRegistryURL = String
  final type APIBaseURL        = String
  final type Topic             = String

  // kafka specific
  val KafkaClientId = "stream-it"
  final type ConsumerGroupId = String
  final type TopicConfigKey  = String

  // for verfications
  final type JsonPathExpr = String

}
