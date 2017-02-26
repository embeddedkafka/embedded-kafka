package net.manub.embeddedkafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer

/** Utility trait for easily creating Kafka consumers and accessing their consumed messages. */
trait Consumers {

  /** Loaner pattern that allows running a code block with a newly created consumer.
    * The consumer's lifecycle will be automatically handled and closed at the end of the
    * given code block.
    *
    * @param block the code block to be executed with the instantiated consumer
    *              passed as an argument
    * @tparam K the type of the consumer's Key
    * @tparam V the type of the consumer's Value
    * @tparam T the type of the block's returning result
    * @return the result of the executed block
    */
  def withConsumer[K: Deserializer, V: Deserializer, T](
      block: KafkaConsumer[K, V] => T)(
      implicit config: EmbeddedKafkaConfig): T = {
    val consumer = newConsumer[K, V]()
    try {
      val result = block(consumer)
      result
    } finally {
      consumer.close()
    }
  }

  /** Convenience alternative to `withConsumer` that offers a consumer for String keys and values.
    *
    * @param block the block to be executed with the consumer
    * @tparam T the type of the result of the code block
    * @return the code block result
    */
  def withStringConsumer[T](block: KafkaConsumer[String, String] => T)(
      implicit config: EmbeddedKafkaConfig): T = {
    import net.manub.embeddedkafka.Codecs.stringDeserializer
    withConsumer(block)
  }

  /** Create a new Kafka consumer.
    *
    * @tparam K the type of the consumer's Key
    * @tparam V the type of the consumer's Value
    * @return the new consumer
    */
  def newConsumer[K: Deserializer, V: Deserializer]()(
      implicit config: EmbeddedKafkaConfig): KafkaConsumer[K, V] = {
    val props = new Properties()
    props.put("group.id", UUIDs.newUuid().toString)
    props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
    props.put("auto.offset.reset", "earliest")

    new KafkaConsumer[K, V](props,
                            implicitly[Deserializer[K]],
                            implicitly[Deserializer[V]])
  }
}
