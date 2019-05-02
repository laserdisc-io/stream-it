package streamit.client

import java.time.{ Instant, ZoneId, ZonedDateTime, Duration => JavaDuration }
import java.util.{ Properties, Collection => JavaCollection }

import cats.effect._
import cats.syntax.apply._
import cats.syntax.flatMap._
import fs2.Stream
import fs2.kafka.{ KafkaConsumer => _ }
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import log.effect.LogWriter
import log.effect.fs2.syntax._
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import org.apache.zookeeper.ZooKeeper
import org.slf4j.LoggerFactory
import streamit._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object ApacheKafkaClient {
  def apply[F[_]: ConcurrentEffect: LogWriter](kafkaSettings: KafkaSettings): ApacheKafkaClient[F] =
    new ApacheKafkaClient(kafkaSettings)
}

/**
  * While ideally an fs2-first client such as the fs2-kafka client would be the only one in use by the
  * KafkaTaskRunner, it is quite limited (especially in documentation), so when we need to do lower level
  * consumergroup manipulations, we need to resort to the standard java client, so that's all being
  * isolated to this client.
  */
class ApacheKafkaClient[F[_]: ConcurrentEffect](settings: KafkaSettings)(
  implicit logger: LogWriter[F]
) {

  // WARNING: Here be dragons.  Or at least, sphincter-tightening jumps between FP and non-FP with
  // ugly java conversions. All help at making this less ugly, very, very welcome.

  // kafka API listeners etc aren't in F[_] so we can't use 'logger' :(
  private[this] val slfLogger = LoggerFactory.getLogger("ApacheKafkaClient")

  /**
    * @return A [[fs2.Stream]] containing a [[org.apache.zookeeper.ZooKeeper]] client, which will be
    *         auto-closed via Stream.bracket.
    */
  def createZookeeperClient(): Stream[F, ZooKeeper] =
    settings.zookeeper match {
      case None =>
        // zookeeper is an optional config, even if the kafkarunner gets configured
        logger
          .infoS("Zookeeper host not configured, skipping ZK test")
          .flatMap(_ => Stream.empty)
      case Some(zkHost) =>
        Stream.bracketCase(
          ConcurrentEffect[F].delay(new ZooKeeper(zkHost, 10000, null)) <*
            logger.info(s"Created zookeeper client to talk to zk host: $zkHost")
        )(
          (ac, _) =>
            ConcurrentEffect[F].delay(ac.close()) >> logger.info(s"Zookeeper client Closed")
        )
    }

  /**
    * @return A [[fs2.Stream]] containing an [[org.apache.kafka.clients.admin.AdminClient]], which will be
    *   auto-closed via Stream.bracket.
    */
  def createAdminClient(): Stream[F, AdminClient] = {
    val properties = new Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers)
    Stream.bracketCase(
      ConcurrentEffect[F].delay(AdminClient.create(properties)) <*
        logger.info(
          s"Created admin client to talk to bootstrap server: ${settings.bootstrapServers}"
        )
    )((ac, _) => ConcurrentEffect[F].delay(ac.close()) >> logger.info(s"Admin client closed"))
  }

  /**
    * @return A [[fs2.Stream]] containing an [[org.apache.kafka.clients.consumer.KafkaConsumer]], which will be
    *   auto-closed via Stream.bracket.
    */
  def createConsumer[K, V](
    pollSize: Option[Int] = None,
    bootstrapServers: Option[BootstrapServers] = None
  )(implicit keyDes: Deserializer[K], valDes: Deserializer[V]): Stream[F, KafkaConsumer[K, V]] = {

    val initialProps = new Properties()
    initialProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000")
    initialProps.put(ConsumerConfig.CLIENT_ID_CONFIG, KafkaClientId)
    initialProps.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId.toString)
    initialProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDes.getClass)
    initialProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDes.getClass)
    pollSize.map(f => initialProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, f.toString))
    initialProps
      .put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, settings.schemaRegistry)

    Stream(initialProps)
      .map(props => {
        props.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          bootstrapServers.getOrElse(settings.bootstrapServers)
        )
        props
      })
      .flatMap(props => {
        Stream.bracketCase(
          ConcurrentEffect[F].delay(new KafkaConsumer[K, V](props)) <*
            logger.info(
              s"Created consumer to bootstrap server: ${settings.bootstrapServers}, listing topics"
            )
        )((c, _) => ConcurrentEffect[F].delay(c.close()) >> logger.info("Consumer closed"))
      })
  }

  /**
    * @return A [[fs2.Stream]] containing an [[org.apache.kafka.clients.consumer.KafkaConsumer]], which will be
    *   auto-closed via Stream.bracket.
    */
  def createConsumerForTopic[K, V](
    topic: Topic,
    offset: Option[Duration] = None,
    pollSize: Option[Int] = None
  )(implicit keyDes: Deserializer[K], valDes: Deserializer[V]): Stream[F, KafkaConsumer[K, V]] = {

    def resetOffsetListener(
      desc: String,
      offsetChangeFunc: JavaCollection[TopicPartition] => Unit,
    ): ConsumerRebalanceListener =
      new ConsumerRebalanceListener {

        override def onPartitionsAssigned(partitions: JavaCollection[TopicPartition]): Unit = {

          val prettyFmt = partitions.asScala.groupBy(_.topic).map {
            case (t, ids) =>
              s"topic:$t, partitions: ${ids.map(_.partition()).toSeq.sorted.mkString(",")}"
          }
          slfLogger.info(s"Running rebalance listener '$desc' for: $prettyFmt")
          offsetChangeFunc(partitions)
        }

        override def onPartitionsRevoked(partitions: JavaCollection[TopicPartition]): Unit = ()
      }

    createConsumer[K, V](pollSize)
      .map(consumer => {

        val offsetListener = offset match {

          case None =>
            resetOffsetListener(
              "force to latest",
              partitions => {
                slfLogger.debug(
                  s"Forcing partitions to latest for ${partitions.asScala.map(_.partition)}"
                )
                consumer.seekToEnd(partitions)
              }
            )

          case Some(offsetAmt) =>
            def toUTC(millis: Long): ZonedDateTime =
              ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))

            resetOffsetListener(
              s"rewinding to first event sent after $offsetAmt ago",
              partitions => {

                val now        = System.currentTimeMillis()
                val offsetTime = now - offsetAmt.toMillis

                // first, build the parition->desiredtiem query param
                val offsetQuery = partitions.asScala.map(_ -> Long.box(offsetTime)).toMap.asJava

                slfLogger.info(s"Requesting offsets for $offsetAmt ago (= ${toUTC(offsetTime)})")

                // ask kafka for each paritition's offset at that point in time (might be null if no messages since!)
                val pastOffsets = consumer.offsetsForTimes(offsetQuery).asScala

                // seek each parition with an offset to that offset, the rest, scroll to latest.
                pastOffsets.partition { case (_, o) => o != null } match {
                  case (offsetsPresent, notPresent) =>
                    offsetsPresent foreach {
                      case (partition, desiredOffset: Any) =>
                        slfLogger.info(
                          s"'$partition' has events after $offsetAmt ago, seeking to offset " +
                            s"${desiredOffset.offset()} for event at ${toUTC(desiredOffset.timestamp())}"
                        )
                        consumer.seek(partition, desiredOffset.offset())
                    }

                    if (notPresent.nonEmpty) {
                      // in the case there's no offset for this timestamp, we force the consumer to the end
                      // to ensure the poll() finds nothing in the case of an existing consumer group id
                      slfLogger.debug(
                        s"Seeking to end for following partitions as they contain no events since " +
                          s"$offsetAmt ago (${toUTC(offsetTime)}: ${notPresent.keys}"
                      )
                      consumer.seekToEnd(notPresent.keys.asJavaCollection)
                    }
                }
              }
            )
        }

        consumer.subscribe(Seq(topic).asJava, offsetListener)

        slfLogger.warn(
          s"Returning a custom offset consumer which won't modify offsets until you poll()!"
        )

        consumer
      })
  }

  def forceConsumerToLatest[K, V](
    topic: Topic
  )(implicit keyDes: Deserializer[K], valDes: Deserializer[V]): Stream[F, Unit] =
    createConsumerForTopic[K, V](topic, pollSize = Some(1))
      .evalTap(
        _ => logger.info(s"Forcing consumer to latest for $topic")
      )
      .map(consumer => {
        consumer.poll(JavaDuration.ofSeconds(5)).records(topic).asScala.toSeq
        consumer.commitSync()
        consumer.close()
      })
      .as(())

  def pollWithOffset[K, V](
    topic: Topic,
    offset: Duration,
    pollSize: Option[Int]
  )(implicit keyDes: Deserializer[K], valDes: Deserializer[V]): Stream[F, ConsumerRecord[K, V]] =
    createConsumerForTopic[K, V](topic, offset = Some(offset), pollSize = pollSize)
      .evalTap(
        _ =>
          logger.info(
            s"Polling $topic, offset=$offset, pollSize=${pollSize.fold("default") { _.toString }}"
        )
      )
      .map(consumer => {
        val recs = consumer.poll(JavaDuration.ofSeconds(5)).records(topic).asScala.toSeq
        consumer.commitSync()
        consumer.close()
        recs
      })
      .evalTap(recs => {
        val headOffset = recs.headOption.fold("n/a") { _.offset.toString }
        val lastOffset = recs.lastOption.fold("n/a") { _.offset.toString }
        logger.info(s"Finished poll, got ${recs.size}") >>
          logger.debug(s"Offset of first record: $headOffset, last offset $lastOffset")
      })
      .flatMap(recs => Stream(recs: _*))

}
