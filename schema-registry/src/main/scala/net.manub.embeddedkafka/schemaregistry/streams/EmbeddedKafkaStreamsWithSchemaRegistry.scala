package net.manub.embeddedkafka.schemaregistry.streams

import net.manub.embeddedkafka.schemaregistry.{
  EmbeddedKafkaConfigWithSchemaRegistry,
  EmbeddedKafkaWithSchemaRegistry
}
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsSupport

trait EmbeddedKafkaStreamsWithSchemaRegistry
    extends EmbeddedKafkaStreamsSupport[EmbeddedKafkaConfigWithSchemaRegistry]
    with EmbeddedKafkaWithSchemaRegistry {

  override protected[embeddedkafka] val streamsConfig =
    new EmbeddedStreamsConfigWithSchemaRegistry

}
