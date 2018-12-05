package net.manub.embeddedkafka.schemaregistry.streams

import net.manub.embeddedkafka.schemaregistry.{
  EmbeddedKafkaConfigWithSchemaRegistry,
  EmbeddedKafkaWithSchemaRegistry
}
import net.manub.embeddedkafka.streams.EmbeddedStreamsConfig

final class EmbeddedStreamsConfigWithSchemaRegistry
    extends EmbeddedStreamsConfig[EmbeddedKafkaConfigWithSchemaRegistry] {

  override def config(streamName: String, extraConfig: Map[String, AnyRef])(
      implicit kafkaConfig: EmbeddedKafkaConfigWithSchemaRegistry)
    : Map[String, AnyRef] =
    baseStreamConfig(streamName) ++ EmbeddedKafkaWithSchemaRegistry.consumerConfigForSchemaRegistry ++ extraConfig

}
