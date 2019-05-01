package streamit

import io.circe.Json

import scala.concurrent.duration._
import scala.util.matching.Regex

/**
  * base trait for all tasks
  */
sealed trait Task extends Product with Serializable {
  def desc: String
}

/**
  * Superclass of all kafka related tasks
  */
sealed trait KafkaTask extends Task

/**
  * Superclass of all redis related tasks
  */
sealed trait RedisTask extends Task

/**
  * Superclass of all rest related tasks
  */
sealed trait ApiTask extends Task

/**
  * Superclass of all Ger, non-system specific tasks
  */
sealed trait GeneralTask extends Task

final case class Sleep(duration: FiniteDuration) extends GeneralTask {
  override def desc: String = s"Sleep for $duration"
}

/**
  * Initialize the kafka consumer at the 'latest' offset for the given topic/group (this forces the
  * consumer to the very latest offsets, regardless of whether existing offsets were present).
  *
  * @param topic The specified topic
  */
final case class KafkaConsumerToLatest(topic: Topic) extends KafkaTask {
  override val desc: String = s"Move consumer to latest for $topic"
}

final case class KafkaSystemCheck() extends KafkaTask {
  override def desc: String = s"Check that the configured kafka system is healthy"
}

/**
  * Represents a key:value comparison to be performed against a kafka topic's configuration
  * @param key The config key, which should exist
  * @param expectedValue If `Some(v)`, then the config value for 'key' should match 'v'.  If `None`,
  *                      but the config has a value for `key`, then this would indicate an error.
  */
final case class TopicConfigCheck(key: TopicConfigKey, expectedValue: Option[String]) {
  override def toString: String = s"'$key'->${expectedValue.getOrElse("[UNSET]")}"
}

final case class KafkaTopicConfigCheck(
  topic: Topic,
  partitionCount: Int,
  replicationFactor: Int,
  configChecks: Seq[TopicConfigCheck]
) extends KafkaTask {
  override def desc: String =
    s"Verify '$topic' has config: [" +
      s"#partitions:$partitionCount" +
      s", repl.factor:$replicationFactor" +
      (if (configChecks.nonEmpty) { s", configChecks:$configChecks}}" }) +
      s"]"
}

sealed trait KafkaTopicContentCheck extends KafkaTask {
  def topic: Topic
  def eventsSince: FiniteDuration
  def timeout: FiniteDuration
  override def desc: String =
    s"Verify '$topic' has " +
      s"events newer than $eventsSince (timeout:$timeout)"
}

final case class KafkaStringTopicContentCheck(
  override val topic: Topic,
  override val eventsSince: FiniteDuration,
  override val timeout: FiniteDuration = 10.seconds,
  keyRegex: Option[Regex] = None,
  checkBodyIsJson: Boolean = false,
) extends KafkaTopicContentCheck {
  override def desc: String = {
    val keyInfo = keyRegex.fold("") { r =>
      s",a key matching '$r'"
    }
    val bodyInfo = if (checkBodyIsJson) ",a valid json value" else ""
    s"${super.desc}$keyInfo$bodyInfo"
  }
}

trait JsonPathCheck extends Product with Serializable {
  val jsonPath: JsonPathExpr
}
final case class JsonPathEquals(override val jsonPath: JsonPathExpr, value: String)
    extends JsonPathCheck
final case class JsonPathMatches(override val jsonPath: JsonPathExpr, value: Regex)
    extends JsonPathCheck

final case class KafkaAvroTopicContentCheck(
  override val topic: Topic,
  override val eventsSince: FiniteDuration,
  override val timeout: FiniteDuration,
  keyChecks: Seq[JsonPathCheck],
  valueChecks: Seq[JsonPathCheck],
) extends KafkaTopicContentCheck {
  override def desc: String = {
    val kcInfo = if (keyChecks.nonEmpty) s",key checks=[${keyChecks.mkString(",")}]" else ""
    val kvInfo = if (valueChecks.nonEmpty) s",value checks=[${valueChecks.mkString(",")}]" else ""
    s"${super.desc}$kcInfo$kvInfo"
  }
}

/**
  * Publish an event to the specified kafka topic
  *
  * @param topic   The destination topic
  * @param key     The event key
  * @param payload The event payload
  */
final case class KafkaProduceString(
  override val desc: String,
  topic: Topic,
  key: String,
  payload: Json
) extends KafkaTask

/**
  * Verify that the redis instance contains this value for the specified key
  *
  * @param key      The redis key
  * @param expected The expected value
  */
final case class RedisGetAndVerify(override val desc: String, key: String, expected: Json)
    extends RedisTask

/**
  * Verify that the event was published onto the specified topic.  This verification step will decode an
  * avro-aware event, but then convert to JSON for verification
  *
  * @param topic    The topic to consume
  * @param key The expected key JSON
  * @param value The expected value JSON
  * @param timeout How long to wait for the expected event before failing the task
  */
final case class KafkaAvroConsumeAndVerify(
  override val desc: String,
  topic: Topic,
  key: Json,
  value: Json,
  timeout: FiniteDuration = 180.seconds
) extends KafkaTask

/**
  * Verify that a GET call to the specified URI on the configured REST API will return the expected value
  *
  * @param uri      The URI on the configured REST API base URL to query
  * @param expected The expected value
  */
final case class ApiGETAndVerify(
  override val desc: String,
  uri: String,
  expected: Json,
  headers: Map[String, String] = Map()
) extends ApiTask
