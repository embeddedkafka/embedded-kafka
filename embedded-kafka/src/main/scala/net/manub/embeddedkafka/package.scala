package net.manub

package object embeddedkafka {

  @deprecated("Use io.github.embeddedkafka.EmbeddedKafka instead", "2.8.1")
  type EmbeddedKafka = io.github.embeddedkafka.EmbeddedKafka
  @deprecated(
    "Use io.github.embeddedkafka.EmbeddedKafkaConfig instead",
    "2.8.0"
  )
  type EmbeddedKafkaConfig = io.github.embeddedkafka.EmbeddedKafkaConfig
  @deprecated(
    "Use io.github.embeddedkafka.EmbeddedKafkaConfigImpl instead",
    "2.8.0"
  )
  type EmbeddedKafkaConfigImpl = io.github.embeddedkafka.EmbeddedKafkaConfigImpl
  @deprecated("Use io.github.embeddedkafka.EmbeddedZ instead", "2.8.1")
  type EmbeddedZ = io.github.embeddedkafka.EmbeddedZ
  @deprecated("Use io.github.embeddedkafka.EmbeddedK instead", "2.8.1")
  type EmbeddedK = io.github.embeddedkafka.EmbeddedK
  @deprecated(
    "Use io.github.embeddedkafka.KafkaUnavailableException instead",
    "2.8.0"
  )
  type KafkaUnavailableException =
    io.github.embeddedkafka.KafkaUnavailableException

  @deprecated("Use io.github.embeddedkafka.Codecs instead", "2.8.1")
  val Codecs = io.github.embeddedkafka.Codecs
  @deprecated("Use io.github.embeddedkafka.ConsumerExtensions instead", "2.8.1")
  val ConsumerExtensions = io.github.embeddedkafka.ConsumerExtensions
  @deprecated("Use io.github.embeddedkafka.EmbeddedKafka instead", "2.8.1")
  val EmbeddedKafka = io.github.embeddedkafka.EmbeddedKafka
  @deprecated(
    "Use io.github.embeddedkafka.EmbeddedKafkaConfig instead",
    "2.8.0"
  )
  val EmbeddedKafkaConfig = io.github.embeddedkafka.EmbeddedKafkaConfig
  @deprecated("Use io.github.embeddedkafka.EmbeddedZ instead", "2.8.1")
  val EmbeddedZ = io.github.embeddedkafka.EmbeddedZ
  @deprecated("Use io.github.embeddedkafka.EmbeddedK instead", "2.8.1")
  val EmbeddedK = io.github.embeddedkafka.EmbeddedK
  @deprecated("Use io.github.embeddedkafka.UUIDs instead", "2.8.1")
  val UUIDs = io.github.embeddedkafka.UUIDs

}
