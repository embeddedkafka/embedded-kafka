package net.manub.embeddedkafka

package object connect {

  @deprecated(
    "Use io.github.embeddedkafka.connect.EmbeddedConnectConfigImpl instead",
    "2.8.0"
  )
  type EmbeddedConnectConfigImpl =
    io.github.embeddedkafka.connect.EmbeddedConnectConfigImpl
  @deprecated(
    "Use io.github.embeddedkafka.connect.EmbeddedKafkaConnect instead",
    "2.8.0"
  )
  type EmbeddedKafkaConnect =
    io.github.embeddedkafka.connect.EmbeddedKafkaConnect

  @deprecated(
    "Use io.github.embeddedkafka.connect.EmbeddedKafkaConnect instead",
    "2.8.0"
  )
  val EmbeddedKafkaConnect =
    io.github.embeddedkafka.connect.EmbeddedKafkaConnect
}
