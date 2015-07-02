package net.manub.embeddedkafka

case class EmbeddedKafkaConfig(kafkaPort: Int = 6001)

object EmbeddedKafkaConfig {
  implicit val defaultConfig = EmbeddedKafkaConfig()
}