package io.github.embeddedkafka

trait EmbeddedKafkaConfig {
  def kafkaPort: Int
  def controllerPort: Int
  def customBrokerProperties: Map[String, String]
  def customProducerProperties: Map[String, String]
  def customConsumerProperties: Map[String, String]
  def numberOfThreads: Int
}

case class EmbeddedKafkaConfigImpl(
    kafkaPort: Int,
    controllerPort: Int,
    customBrokerProperties: Map[String, String],
    customProducerProperties: Map[String, String],
    customConsumerProperties: Map[String, String]
) extends EmbeddedKafkaConfig {
  override val numberOfThreads: Int = 2
}

object EmbeddedKafkaConfig {
  lazy val defaultKafkaPort      = 6001
  lazy val defaultControllerPort = 6002

  implicit val defaultConfig: EmbeddedKafkaConfig = apply()

  def apply(
      kafkaPort: Int = defaultKafkaPort,
      controllerPort: Int = defaultControllerPort,
      customBrokerProperties: Map[String, String] = Map.empty,
      customProducerProperties: Map[String, String] = Map.empty,
      customConsumerProperties: Map[String, String] = Map.empty
  ): EmbeddedKafkaConfig =
    EmbeddedKafkaConfigImpl(
      kafkaPort,
      controllerPort,
      customBrokerProperties,
      customProducerProperties,
      customConsumerProperties
    )
}
