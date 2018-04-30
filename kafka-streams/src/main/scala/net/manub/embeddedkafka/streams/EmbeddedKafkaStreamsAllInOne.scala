package net.manub.embeddedkafka.streams

import net.manub.embeddedkafka.{Consumers, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.streams.Topology
import org.scalatest.Suite

/** Convenience trait for testing Kafka Streams with ScalaTest.
  * It exposes `EmbeddedKafkaStreams.runStreams` as well as `Consumers` api
  * for easily creating and querying consumers in tests.
  *
  * @see [[Consumers]]
  * @see [[EmbeddedKafkaStreams]]
  */
trait EmbeddedKafkaStreamsAllInOne extends EmbeddedKafkaStreams with Consumers {
  this: Suite =>

  /** Run Kafka Streams while offering a String-based consumer for easy testing of stream output.
    *
    * @param topicsToCreate the topics that should be created. Usually these should be the topics
    *                       that the Streams-under-test use for inputs and outputs. They need to be
    *                       created before running the streams and
    *                       this is automatically taken care of.
    * @param topology       the streams topology that will be instantiated
    * @param block          the block of testing code that will be executed by passing the simple
    *                       String-based consumer.
    * @return the result of the testing code
    */
  def runStreamsWithStringConsumer(
      topicsToCreate: Seq[String],
      topology: Topology)(block: KafkaConsumer[String, String] => Any)(
      implicit config: EmbeddedKafkaConfig): Any =
    runStreams(topicsToCreate, topology)(withStringConsumer[Any](block))(config)
}
