package net.manub.embeddedkafka.schemaregistry.streams

import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaConfigWithSchemaRegistry
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOneSupport

/** Convenience trait exposing [[EmbeddedKafkaStreamsWithSchemaRegistry.runStreams]]
  * as well as [[net.manub.embeddedkafka.Consumers]] api for easily creating and
  * querying consumers.
  *
  * @see [[EmbeddedKafkaStreamsWithSchemaRegistry]]
  * @see [[net.manub.embeddedkafka.Consumers]]
  */
trait EmbeddedKafkaStreamsWithSchemaRegistryAllInOne
    extends EmbeddedKafkaStreamsAllInOneSupport[
      EmbeddedKafkaConfigWithSchemaRegistry]
    with EmbeddedKafkaStreamsWithSchemaRegistry
