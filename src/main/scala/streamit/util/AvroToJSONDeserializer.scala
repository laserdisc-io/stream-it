package streamit.util

import java.util.{ Map => JMap }

import io.circe.Json
import io.circe.parser._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory
import streamit.SchemaRegistryURL

import scala.collection.JavaConverters._

/**
  * A serializer which deserializes keys and values from an avro-aware topic, but marshals into circe [[Json]]
  * objects, instead of requiring knowledge/access to avro model classes.  Allows the test to be as
  * use-case agnostic as possible.
  */
class AvroToJSONDeserializer(schemaRegistry: Option[SchemaRegistryURL]) extends Deserializer[Json] {

  // no logger in F[_] available here :(
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /* Yes, avroDes is a var :(  This class is written to be compatible with both:
   * - apache-kafka, which takes SerDe classnames via properties, and calls configure() to configure
   * - fs2-kafka, which takes initialized instances of the deserializer (but doesn't call configure())
   *
   * This class supports both modes, but means that, in the case of the configure() route, the avro deserializer
   * cannot be built until configure provides the schema registry url (The fs2 use case provides it in the constructor)
   *
   * TODO: find a 'safer' solution, but ensure that use cases for both clients still work afterwards :)
   */
  var avroDes: KafkaAvroDeserializer = _
  schemaRegistry match {
    case Some(url) => init(url)
    case None =>
      logger.debug(
        "Constructed without registry url. Invoke configure() or init() before deserialization"
      )
  }

  def this(schemaRegistry: SchemaRegistryURL) {
    this(Some(schemaRegistry))
  }

  def this() {
    this(None)
  }

  def init(schemaRegistryURL: SchemaRegistryURL): Unit = {
    avroDes = new KafkaAvroDeserializer(new CachedSchemaRegistryClient(schemaRegistryURL, 1000))
    logger.info(s"Initialized with schema registry: $schemaRegistryURL")
  }

  override def configure(configs: JMap[String, _], isKey: Boolean): Unit =
    configs.asScala.get(SCHEMA_REGISTRY_URL_CONFIG).map(_.toString) match {
      case Some(url) => init(url)
      case _         =>
        // would appear that configure() gets called several times :rolleyes: so don't throw
        logger.warn(
          s"Configure called, but $SCHEMA_REGISTRY_URL_CONFIG not present (saw: ${configs.asScala})"
        )
    }

  override def deserialize(topic: String, data: Array[Byte]): Json =
    try {

      if (avroDes == null) {
        throw new IllegalStateException(
          "Cannot deserialize() until configure() " +
            s"is called with a value for $SCHEMA_REGISTRY_URL_CONFIG"
        )
      }

      val genericRecord = avroDes.deserialize(topic, data).asInstanceOf[GenericRecord]
      parse(genericRecord.toString) match {
        case Right(json) => json
        case Left(failure) =>
          logger.error(s"Failed to parse JSON from message: $failure - message was: $genericRecord")
          Json.Null
      }

    } catch {
      case e: Throwable =>
        logger.error(s"Failure to deserialize message from topic $topic - $e", e)
        Json.Null
    }

  override def close(): Unit = Option(avroDes) match {
    case Some(a) => a.close()
    case None    => logger.debug("close() invoked but no avrodes was created")
  }
}
