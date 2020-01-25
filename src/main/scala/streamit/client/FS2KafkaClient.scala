package streamit.client

import cats.effect.{ ConcurrentEffect, _ }
import fs2.Stream
import fs2.kafka._
import log.effect.LogWriter
import log.effect.fs2.syntax._
import streamit.{ KafkaSettings, _ }

object FS2KafkaClient {
  def apply[F[_]: ConcurrentEffect: ContextShift: Timer: LogWriter](
    settings: KafkaSettings
  ): FS2KafkaClient[F] = new FS2KafkaClient(settings)
}

/**
  * All fs2-kafka related activities
  */
class FS2KafkaClient[F[_]: ConcurrentEffect: ContextShift: Timer](settings: KafkaSettings)(
  implicit logger: LogWriter[F]
) {

  def createConsumer[K, V](
    topic: String
  )(
    implicit keyDes: Deserializer[F, K],
    valDes: Deserializer[F, V]
  ): Stream[F, CommittableConsumerRecord[F, K, V]] =
    logger.infoS(
      s"Creating consumer in ${settings.groupId} for topic $topic on server: ${settings.bootstrapServers}"
    ) >>
      consumerStream[F]
        .using(
          ConsumerSettings[F, K, V](keyDes, valDes)
            .withAutoOffsetReset(AutoOffsetReset.Latest)
            .withClientId(KafkaClientId)
            .withBootstrapServers(settings.bootstrapServers)
            .withGroupId(settings.groupId.toString)
        )
        .evalTap(_.subscribeTo(topic))
        .flatMap(_.stream)
        .mapAsync(maxConcurrent = 25) { event =>
          ConcurrentEffect[F].pure(event)
        }

  def createProducer[K, V](
    implicit keySer: Serializer[F, K],
    valSet: Serializer[F, V]
  ): Stream[F, KafkaProducer[F, K, V]] =
    logger.infoS(s"Creating producer to bootstrap server ${settings.bootstrapServers}") >>
      producerStream[F]
        .using(
          ProducerSettings(keySer, valSet)
            .withMaxInFlightRequestsPerConnection(1)
            .withBootstrapServers(settings.bootstrapServers)
            .withClientId(KafkaClientId)
            .withAcks(Acks.All)
        )

}
