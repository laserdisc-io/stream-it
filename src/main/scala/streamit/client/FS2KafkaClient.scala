package streamit.client

import streamit._
import cats.effect.{ ConcurrentEffect, _ }
import fs2.Stream
import fs2.kafka.{ Serializer => _, _ }
import log.effect.LogWriter
import log.effect.fs2.syntax._
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import streamit.KafkaSettings

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
    implicit keyDes: Deserializer[K],
    valDes: Deserializer[V]
  ): Stream[F, CommittableMessage[F, K, V]] =
    logger.infoS(
      s"Creating consumer in ${settings.groupId} for topic $topic on server: ${settings.bootstrapServers}"
    ) >>
      (for {
        ec <- consumerExecutionContextStream[F]
        event <- consumerStream[F]
                  .using(
                    ConsumerSettings[K, V](keyDes, valDes, ec)
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

      } yield event)

  def createProducer[K, V](
    implicit keySer: Serializer[K],
    valSet: Serializer[V]
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
