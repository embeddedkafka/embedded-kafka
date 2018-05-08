package net.manub.embeddedkafka.schemaregistry.streams

import net.manub.embeddedkafka.UUIDs
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaWithSchemaRegistry.consumerConfigForSchemaRegistry
import net.manub.embeddedkafka.schemaregistry.{
  EmbeddedKafkaConfigWithSchemaRegistry,
  EmbeddedKafkaWithSchemaRegistry
}
import net.manub.embeddedkafka.streams.TestStreamsConfig
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.scalatest.Suite

// TODO: need to find a better way of not duplicating this code from the kafka-streams module

/** Helper trait for testing Kafka Streams.
  * It creates an embedded Kafka Instance for each test case.
  * Use `runStreams` to execute your streams.
  */
trait EmbeddedKafkaStreamsWithSchemaRegistry
    extends EmbeddedKafkaWithSchemaRegistry
    with TestStreamsConfig {
  this: Suite =>

  /** Execute Kafka streams and pass a block of code that can
    * operate while the streams are active.
    * The code block can be used for publishing and consuming messages in Kafka.
    *
    * @param topicsToCreate the topics that should be created in Kafka before launching the streams.
    * @param topology       the streams topology that will be used to instantiate the streams with
    *                       a default configuration (all state directories are different and
    *                       in temp folders)
    * @param extraConfig    additional KafkaStreams configuration (overwrite existing keys in
    *                       default config)
    * @param block          the code block that will executed while the streams are active.
    *                       Once the block has been executed the streams will be closed.
    */
  def runStreams[T](topicsToCreate: Seq[String],
                    topology: Topology,
                    extraConfig: Map[String, AnyRef] = Map.empty)(block: => T)(
      implicit config: EmbeddedKafkaConfigWithSchemaRegistry): T =
    withRunningKafka {
      topicsToCreate.foreach(topic => createCustomTopic(topic))
      val streamId = UUIDs.newUuid().toString
      val streams =
        new KafkaStreams(
          topology,
          streamConfig(streamId,
                       extraConfig ++ consumerConfigForSchemaRegistry))
      streams.start()
      try {
        block
      } finally {
        streams.close()
      }
    }(config)
}
