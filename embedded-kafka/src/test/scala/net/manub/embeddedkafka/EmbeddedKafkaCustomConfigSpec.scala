package net.manub.embeddedkafka

import scala.language.postfixOps

class EmbeddedKafkaCustomConfigSpec extends EmbeddedKafkaSpecSupport with EmbeddedKafka {
  val TwoMegabytes = 2097152
  val ThreeMegabytes = 3145728

  "the custom config" should {
    "allow pass additional producer parameters" in {
      val customBrokerConfig = Map(
        "replica.fetch.max.bytes" -> s"$ThreeMegabytes",
        "message.max.bytes" -> s"$ThreeMegabytes")

      val customProducerConfig = Map("max.request.size" -> s"$ThreeMegabytes")
      val customConsumerConfig = Map("max.partition.fetch.bytes" -> s"$ThreeMegabytes")

      implicit val customKafkaConfig = EmbeddedKafkaConfig(
        customBrokerProperties = customBrokerConfig,
        customProducerProperties = customProducerConfig,
        customConsumerProperties = customConsumerConfig)

      val bigMessage = generateMessageOfLength(TwoMegabytes)
      val topic = "big-message-topic"

      withRunningKafka {
        publishStringMessageToKafka(topic, bigMessage)
        consumeFirstStringMessageFrom(topic) shouldBe bigMessage
      }
    }
  }

  def generateMessageOfLength(length: Int): String = Stream.continually(util.Random.nextPrintableChar) take length mkString
}
