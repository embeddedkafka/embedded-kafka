package io.github.embeddedkafka

import io.github.embeddedkafka.EmbeddedKafka._
import io.github.embeddedkafka.EmbeddedKafkaConfig.{
  defaultControllerPort,
  defaultKafkaPort
}
import io.github.embeddedkafka.EmbeddedKafkaSpecSupport._
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}
import org.scalatest.OptionValues

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class EmbeddedKafkaObjectSpec
    extends EmbeddedKafkaSpecSupport
    with OptionValues {
  val consumerPollTimeout: FiniteDuration = 5.seconds

  "the EmbeddedKafka object" when {
    "invoking the start and stop methods" should {
      "start and stop Kafka broker and controller on the default ports" in {
        EmbeddedKafka.start()

        expectedServerStatus(defaultControllerPort, Available)
        expectedServerStatus(defaultKafkaPort, Available)

        EmbeddedKafka.stop()

        expectedServerStatus(defaultKafkaPort, NotAvailable)
        expectedServerStatus(defaultControllerPort, NotAvailable)
      }

      "start and stop Kafka broker and controller on different specified ports using an implicit configuration" in {
        implicit val config: EmbeddedKafkaConfig =
          EmbeddedKafkaConfig(kafkaPort = 12345, controllerPort = 54321)
        EmbeddedKafka.start()

        expectedServerStatus(12345, Available)
        expectedServerStatus(54321, Available)

        EmbeddedKafka.stop()
      }

      "start and stop a specific Kafka" in {
        val firstServer = EmbeddedKafka.start()(
          EmbeddedKafkaConfig(kafkaPort = 7000, controllerPort = 7001)
        )
        EmbeddedKafka.start()(
          EmbeddedKafkaConfig(kafkaPort = 8000, controllerPort = 8001)
        )

        expectedServerStatus(7001, Available)
        expectedServerStatus(7000, Available)

        expectedServerStatus(8001, Available)
        expectedServerStatus(8000, Available)

        EmbeddedKafka.stop(firstServer)

        expectedServerStatus(7000, NotAvailable)
        expectedServerStatus(7001, NotAvailable)

        expectedServerStatus(8000, Available)
        expectedServerStatus(8001, Available)

        EmbeddedKafka.stop()
      }

      "start and stop Kafka broker and controller successfully on arbitrary available ports" in {
        val someConfig =
          EmbeddedKafkaConfig(kafkaPort = 0, controllerPort = 0)
        val kafka = EmbeddedKafka.start()(someConfig)

        val usedControllerPort = EmbeddedKafka.controllerPort(kafka.controller)
        val usedKafkaPort      = EmbeddedKafka.kafkaPort(kafka.broker)

        expectedServerStatus(usedControllerPort, Available)
        expectedServerStatus(usedKafkaPort, Available)

        kafka.config.controllerPort should be(usedControllerPort)
        kafka.config.kafkaPort should be(usedKafkaPort)

        EmbeddedKafka.stop()

        expectedServerStatus(usedKafkaPort, NotAvailable)
        expectedServerStatus(usedControllerPort, NotAvailable)
      }

      "start and stop multiple Kafka instances on specified ports" in {
        val someConfig =
          EmbeddedKafkaConfig(kafkaPort = 12345, controllerPort = 54321)
        val someBroker = EmbeddedKafka.start()(someConfig)

        val someOtherConfig =
          EmbeddedKafkaConfig(kafkaPort = 23456, controllerPort = 65432)
        val someOtherBroker = EmbeddedKafka.start()(someOtherConfig)

        val topic            = "publish_test_topic_1"
        val someOtherMessage = "another message!"

        val serializer   = new StringSerializer
        val deserializer = new StringDeserializer

        publishToKafka(topic, "hello world!")(someConfig, serializer)
        publishToKafka(topic, someOtherMessage)(someOtherConfig, serializer)

        expectedServerStatus(someConfig.kafkaPort, Available)
        EmbeddedKafka.stop(someBroker)

        val moreRecords =
          withConsumer[String, String, Iterable[String]] { anotherConsumer =>
            anotherConsumer.subscribe(List(topic).asJava)
            anotherConsumer
              .poll(duration2JavaDuration(consumerPollTimeout))
              .records(topic)
              .asScala
              .map(Codecs.stringValueCrDecoder)
          }(someOtherConfig, deserializer, deserializer)

        moreRecords.size shouldBe 1
        moreRecords.headOption.value shouldBe someOtherMessage

        EmbeddedKafka.stop(someOtherBroker)
      }
    }

    "invoking the isRunning method" should {
      "return true when Kafka is running" in {
        EmbeddedKafka.start()
        EmbeddedKafka.isRunning shouldBe true
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }
    }
  }
}
