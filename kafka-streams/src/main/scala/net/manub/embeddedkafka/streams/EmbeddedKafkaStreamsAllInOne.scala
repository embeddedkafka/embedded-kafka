package net.manub.embeddedkafka.streams

import net.manub.embeddedkafka.{Consumers, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.streams.Topology

/** Convenience trait exposing [[EmbeddedKafkaStreamsSupport.runStreams]]
  * as well as [[Consumers]] api for easily creating and
  * querying consumers.
  *
  * @see [[Consumers]]
  * @see [[EmbeddedKafkaStreamsSupport]]
  */
trait EmbeddedKafkaStreamsAllInOne
    extends EmbeddedKafkaStreamsAllInOneSupport[EmbeddedKafkaConfig]
    with EmbeddedKafkaStreams

private[embeddedkafka] trait EmbeddedKafkaStreamsAllInOneSupport[
    C <: EmbeddedKafkaConfig]
    extends Consumers {
  this: EmbeddedKafkaStreamsSupport[C] =>

  /** Run Kafka Streams while offering a String-based consumer.
    *
    * @param topicsToCreate the topics that should be created. Usually these should be the topics
    *                       that the stream use for inputs and outputs. They need to be
    *                       created before running the streams and
    *                       this is automatically taken care of.
    * @param topology       the streams topology that will be instantiated
    * @param block          the block of code that will be executed by passing the simple
    *                       String-based consumer.
    * @return
    */
  def runStreamsWithStringConsumer(topicsToCreate: Seq[String],
                                   topology: Topology)(
      block: KafkaConsumer[String, String] => Any)(implicit config: C): Any =
    runStreams(topicsToCreate, topology)(withStringConsumer[Any](block))(config)

}
