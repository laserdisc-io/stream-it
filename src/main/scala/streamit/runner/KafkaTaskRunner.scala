package streamit.runner

import java.util.concurrent.TimeUnit

import cats.effect._
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import streamit.client.{ ApacheKafkaClient, FS2KafkaClient }
import streamit.implicits._
import streamit.util.AvroToJSONDeserializer
import streamit._
import fs2.Stream
import fs2.kafka._
import gnieh.diffson.circe._
import io.circe.Json
import log.effect.LogWriter
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.serialization.{
  Deserializer,
  Serializer,
  StringDeserializer,
  StringSerializer
}
import streamit.runner.Result.{ Failure, Success }

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object KafkaTaskRunner {

  def apply[F[_]: ConcurrentEffect: ContextShift: Timer: LogWriter](
    settings: F[Settings]
  ): Stream[F, KafkaTaskRunner[F]] =
    Stream
      .eval(settings)
      .map(_.kafka)
      .flatMap {
        case Some(ks) => Stream.apply(new KafkaTaskRunnerImpl(ks))
        case None     => Stream.apply(new NoOpKafkaTaskRunnerImpl)
      }
}

sealed trait KafkaTaskRunner[F[_]] extends TaskRunner[F, KafkaTask]

// TODO: extract common NoOp runner and eliminate this
final class NoOpKafkaTaskRunnerImpl[F[_]] extends KafkaTaskRunner[F] {
  def run[TT >: KafkaTask <: Task](task: TT): Stream[F, Result] = fail(task)
}

class KafkaTaskRunnerImpl[F[_]: ConcurrentEffect: ContextShift: Timer](
  kafkaSettings: KafkaSettings
)(
  implicit logger: LogWriter[F]
) extends KafkaTaskRunner[F] {

  /* While we prefer to use the fs2-kafka library exclusively, it has proven quite limited when it comes to
   * lower level operations, such as offset manipulation, so we resort to the java client.  Both of these are
   * isolated to their own classes. */
  private[this] val fs2Client    = FS2KafkaClient[F](kafkaSettings)
  private[this] val apacheClient = ApacheKafkaClient[F](kafkaSettings)

  implicit val stringDes: Deserializer[String] = new StringDeserializer
  implicit val stringSer: Serializer[String]   = new StringSerializer
  implicit val avroDes: Deserializer[Json] = new AvroToJSONDeserializer(
    kafkaSettings.schemaRegistry
  )

  def run[TT >: KafkaTask <: Task](task: TT): Stream[F, Result] =
    task match {
      case t: KafkaConsumerToLatest     => runConsumerToLatest(t)
      case t: KafkaAvroConsumeAndVerify => runAvroConsumeAndVerify(t)
      case t: KafkaTopicContentCheck    => runTopicContentCheck(t)
      case t: KafkaProduceString        => runProduceString(t)
      case t: KafkaSystemCheck          => runSystemCheck(t)
      case t: KafkaTopicConfigCheck     => runTopicConfigCheck(t)
    }

  /**
    * Subscribe the given consumer group ID to the specified topic, and seek to the latest offsets in the
    * partitions assigned.  Normally, with 'auto.offset.reset'=latest, if offsets for this group already
    * exist, those will be used.  This method overrides moving the consumer group offsets to the latest available
    * in the partitions.
    *
    * This is useful for use cases where the test may not have run for some time, and in the meantime a
    * large volume of data has been published to the target topic.  A test might  normally be disinterested
    * in that data, so this lets us blow away any known offsets that would have forced us to consume them.
    *
    */
  def runConsumerToLatest(task: KafkaConsumerToLatest): Stream[F, Result] =
    apacheClient
      .forceConsumerToLatest[String, String](task.topic)
      .evalTap(
        _ =>
          logger.info(s"Consumer group ${kafkaSettings.groupId} offsets for ${task.topic} updated.")
      )
      .as(Success(task))

  /** Consume from the specified topic until an event with the specified key is found, verifying that
    * the provided payload matches, or fail if the timeout is reached.
    */
  def runAvroConsumeAndVerify(
    task: KafkaAvroConsumeAndVerify
  ): Stream[F, Result] =
    task match {

      case KafkaAvroConsumeAndVerify(_, topic, expectedKey, expectedValue, timeout) =>
        fs2Client
          .createConsumer[Json, Json](topic)
          .evalTap(
            event =>
              event.record.key.hcursor.get[String]("orgId").getOrElse("n/a") match {
                case "test" =>
                  logger.info(
                    s"Saw test event, key: ${event.record.key.noSpaces} while looking for ${expectedKey.noSpaces}"
                  )
                case otherOrg =>
                  logger.debug(
                    s"Saw $otherOrg event, key: ${event.record.key.noSpaces} while looking for ${expectedKey.noSpaces}"
                  )
            }
          )
          .filter(_.record.key == expectedKey)
          .runForAtMost(m => m.record.value().canonicalJson == expectedValue.canonicalJson, timeout)
          .evalTap(
            evt =>
              logger.debug(
                s"Key '${expectedKey.noSpaces}' found, testing value: ${evt.record.value.noSpaces}"
            )
          )
          .last
          .map {
            case Some(event) if event.record.value.canonicalJson == expectedValue.canonicalJson =>
              Success(task)
            case Some(event) =>
              Failure(
                task,
                s"""Found expected key "${event.record.key.noSpaces} but value wasn't as expected:",
                   | ------------------------------- Actual JSON ------------------------------- \n
                   | ${event.record.value.canonicalJson.spaces2}\n
                   | ------------------------------ Expected JSON ------------------------------ \n
                   | ${expectedValue.canonicalJson.spaces2}\n
                   | ------- To fix this, your 'expected' needs the following changes: --------- \n
                   | ${JsonDiff.diff(
                     expectedValue.canonicalJson,
                     event.record.value.canonicalJson,
                     remember = false
                   )}\n
                   | --------------------------------------------------------------------------- \n
                   | """.stripMargin
              )
            case None => Failure(task, s"Gave up waiting ($timeout)")
          }
          .handleErrorWith(errToFailure(task))

    }

  /** Produce an event on the specified topic with the provided key and payload
    */
  def runProduceString(task: KafkaProduceString): Stream[F, Result] =
    fs2Client
      .createProducer[String, String]
      .evalMap { producer =>
        producer.produce(
          ProducerMessage.one(ProducerRecord(task.topic, task.key, task.payload.noSpaces))
        ) *>
          logger.info(s"Produced event with key $task.key on topic $task.topic")
      }
      .as(Success(task))

  /** Check that the specified topic contains data _after_ the specified offset.  In the successful
    * case that data was present, perform some optional additional checks, such as key regex and
    * value parsing.
    */
  def runTopicContentCheck[K, V](
    task: KafkaTopicContentCheck
  ): Stream[F, Result] =
    task match {
      case t: KafkaStringTopicContentCheck =>
        runTopicContentCheck[String, String](
          kafkaSettings.groupId,
          t,
          List(verifyBodyJson(t), verifyKeyRegex(t))
        )
      case t: KafkaAvroTopicContentCheck =>
        runTopicContentCheck[Json, Json](kafkaSettings.groupId, t, List(verifyKey(t)))
    }

  def runTopicContentCheck[K, V](
    groupId: ConsumerGroupId,
    task: KafkaTopicContentCheck,
    verifications: List[ConsumerRecord[K, V] => F[Unit]]
  )(implicit keyDes: Deserializer[K], valDes: Deserializer[V]): Stream[F, Result] =
    apacheClient
      .pollWithOffset[K, V](
        task.topic,
        task.eventsSince,
        Some(1)
      )
      .runForAtMost(_ => false, task.timeout)
      .evalTap(
        rec =>
          logger.info(
            s"Event found after ${task.eventsSince} ago on partition:${rec.partition}, " +
              s"offset:${rec.offset()} with key ${rec.key}"
        )
      )
      .evalTap(rec => {
        logger.info(s"Performing ${verifications.size} verifications against record..") *>
          verifications.map(v => v(rec)).sequence *>
          logger.info(s"Verifications completed")
      })
      .last
      .map {
        case Some(_) => Success(task)
        case None =>
          Failure(task, s"Nothing in the last ${task.eventsSince}, timed out (${task.timeout})")
      }
      .handleErrorWith(errToFailure(task))

  /**
    * Verify that the specified topic exists and is configured as expected.
    */
  def runTopicConfigCheck(task: KafkaTopicConfigCheck): Stream[F, Result] = {

    def validateConfigs(config: Map[String, String], checks: Seq[TopicConfigCheck]) =
      checks.flatMap(
        check =>
          config.get(check.key) match {
            case None =>
              Some(s"Topic ${task.topic} missing configuration for key '${check.key}'")
            case Some(actualValue) =>
              check.expectedValue match {
                case None =>
                  Some(
                    s"Topic ${task.topic} had unexpected config for key '${check.key}' (with value '$actualValue')"
                  )
                case Some(expVal) =>
                  actualValue match {
                    case v if v == expVal => None // validation passed!
                    case _ =>
                      Some(
                        s"Topic ${task.topic} had expected config key '${check.key}', " +
                          s"but unexpected value (expected:'$expVal', actual:'$actualValue')"
                      )
                  }
              }
        }
      )

    def validateParititionCount(td: TopicDescription, expectPartitions: Int): Option[String] =
      Option(td.partitions().size() == expectPartitions).collect {
        case false => s"Topic has ${td.partitions().size()} partitions, expected $expectPartitions"
      }

    def validateReplicationFactor(td: TopicDescription, expectReplFactor: Int): Option[String] =
      td.partitions().asScala.headOption match {
        case None => Some("Couldn't check replication factor, no partitions available!")
        case Some(first) =>
          Option(first.replicas().size() == expectReplFactor).collect {
            case false =>
              s"Topic has replication factor ${first.replicas().size()}, expected $expectReplFactor"
          }
      }

    val adminClient = apacheClient.createAdminClient()

    val topicState = adminClient
      .map(_.describeTopics(Seq(task.topic).asJava))
      .map(_.values().get(task.topic).get(5, TimeUnit.SECONDS)) // throws if topic unknown
      .evalTap(
        t =>
          logger.info(s"Loaded info on ${t.partitions().size()} partitions for topic ${t.name()}")
      )

    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, task.topic)
    val topicConfig = adminClient
      .map(_.describeConfigs(Seq(topicResource).asJava))
      .map(_.values().get(topicResource).get(1, TimeUnit.SECONDS)) // throws if topic unknown
      .map(_.entries().asScala.map(e => e.name() -> e.value()).toMap)
      .evalTap(
        tConfig => logger.info(s"Loaded ${tConfig.keys.size} config keys for topic ${task.topic}")
      )

    topicState
      .zip(topicConfig)
      .flatMap {
        case (state, config) =>
          validateConfigs(config, task.configChecks) ++
            Seq(
              validateParititionCount(state, task.partitionCount),
              validateReplicationFactor(state, task.replicationFactor)
            ).flatten match {
            case Nil      => Stream(Success(task))
            case failures => Stream(Failure(task, s"Failed validations: $failures"))
          }
      }
      .handleErrorWith(errToFailure(task))
  }

  /** Perform a system check against the configured system, by requesting a topic listing from
    * each of the provided bootstrap servers individually, and if configured, the zookeeper host
    * directly as well.
    */
  def runSystemCheck(task: KafkaSystemCheck): Stream[F, Result] = {

    val zkCheck = apacheClient
      .createZookeeperClient()
      .map(_.getChildren("/brokers/topics", false))
      .evalTap(topics => logger.info(s"Successfully queried ZK and saw ${topics.size()} topics"))
      .evalTap(topics => logger.debug(s"ZK has topics: ${topics.asScala.toList}"))
      .handleErrorWith(e => errStream(s"ZK check failed: ${e.getMessage}", e))

    val brokerChecks = Stream
      .emits(kafkaSettings.bootstrapServers.split(",").toList)
      .evalTap(brokerList => logger.info(s"Will be checking $brokerList"))
      .flatMap(bs => {
        apacheClient
          .createConsumer[String, String](bootstrapServers = Some(bs))
          .map(_.listTopics(java.time.Duration.ofSeconds(5)))
          .evalTap(
            topics => logger.info(s"Bootstrap server $bs OK, has ${topics.keySet().size()} topics")
          )
          .handleErrorWith(e => errStream(s"Broker check to $bs failed: ${e.getMessage}", e))
      })

    Stream
      .eval(zkCheck.compile.drain >> brokerChecks.compile.drain)
      .as(Success(task))
      .handleErrorWith(errToFailure(task))
  }

  private[this] def verifyKeyRegex(
    task: KafkaStringTopicContentCheck
  ): ConsumerRecord[String, String] => F[Unit] =
    rec =>
      task.keyRegex match {
        case Some(keyRegex) => valueMatches(rec.key(), keyRegex, "while matching event key")
        case _              => logger.info(s"No regex check, key was '${rec.key()}")
    }

  private[this] def verifyBodyJson(
    task: KafkaStringTopicContentCheck
  ): ConsumerRecord[String, String] => F[Unit] =
    rec =>
      if (task.checkBodyIsJson) {
        rec
          .value()
          .asJsonF
          .flatMap(
            json => logger.info(s"Body for key '${rec.key()}' was valid JSON: ${json.spaces2}")
          )
      } else {
        logger.info(
          s"No JSON value check for event with key '${rec.key()}', value was: ${rec.value()}"
        )
    }

  private[this] def verifyKey(
    task: KafkaAvroTopicContentCheck
  ): ConsumerRecord[Json, Json] => F[Unit] =
    rec => {
      verifyChecks("key", rec.key(), task.keyChecks) *>
        verifyChecks("value", rec.value(), task.valueChecks)
    }

  private[this] def verifyChecks(desc: String, json: Json, checks: Seq[JsonPathCheck]): F[Unit] =
    checks match {
      case Nil => logger.info(s"No $desc checks to perform")
      case cks =>
        cks
          .map { ck =>
            val jsonPath = ck.jsonPath
            val result: F[Unit] = json.valueAtPath[String](jsonPath) match {
              case Some(v) =>
                ck match {
                  case c: JsonPathEquals =>
                    valueEquals(
                      v,
                      c.value,
                      s"while comparing jsonPath '$jsonPath' in $desc: '${json.noSpaces}'"
                    )
                  case c: JsonPathMatches =>
                    valueMatches(
                      v,
                      c.value,
                      s"while matching jsonPath '$jsonPath' in $desc: '${json.noSpaces}'"
                    )
                }
              case None => errF(s"No value at JsonPath '$jsonPath' for JSON: ${json.noSpaces}")
            }
            result
          }
          .toList
          .sequence *> logger.info(s"$desc checks completed")
    }

  private[this] def valueMatches(v: String, expected: Regex, context: String): F[Unit] = v match {
    case expected() => logger.info(s"Value '$v' matches regex: $expected ($context)")
    case _          => errF(s"Value '$v' does not match regex '$expected'  ($context)")
  }

  private[this] def valueEquals(value: String, expected: String, context: String): F[Unit] =
    value match {
      case v if v != expected =>
        errF(s"Value '$v' did not match expected value '$expected' ($context)")
      case _ => logger.info(s"Found expected value '$expected' ($context)")
    }

  private[this] def errF[T](msg: String): F[T] = ConcurrentEffect[F].raiseError(new Throwable(msg))
  private[this] def errStream(msg: String, c: Throwable) =
    Stream.raiseError[F](new Throwable(msg, c))
  private[this] def errToFailure(task: Task): Throwable => Stream[F, Failure] =
    e =>
      Stream
        .eval(logger.info(s"Error ${e.getClass.getName}: ${e.getMessage} caused $task to fail:", e))
        .map(_ => Failure(task, s"${e.getClass.getName}: ${e.getMessage}"))
}
