package net.manub.embeddedkafka

package object streams {

  @deprecated(
    "Use io.github.embeddedkafka.streams.EmbeddedKafkaStreams instead",
    "2.8.0"
  )
  type EmbeddedKafkaStreams =
    io.github.embeddedkafka.streams.EmbeddedKafkaStreams
  @deprecated(
    "Use io.github.embeddedkafka.streams.EmbeddedStreamsConfigImpl instead",
    "2.8.0"
  )
  type EmbeddedStreamsConfigImpl =
    io.github.embeddedkafka.streams.EmbeddedStreamsConfigImpl

  @deprecated(
    "Use io.github.embeddedkafka.streams.EmbeddedKafkaStreams instead",
    "2.8.0"
  )
  val EmbeddedKafkaStreams =
    io.github.embeddedkafka.streams.EmbeddedKafkaStreams

}
