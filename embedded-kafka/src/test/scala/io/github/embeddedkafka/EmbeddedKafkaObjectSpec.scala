package io.github.embeddedkafka

import java.nio.file.Files

import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}
import io.github.embeddedkafka.EmbeddedKafka._
import io.github.embeddedkafka.EmbeddedKafkaConfig.{
  defaultKafkaPort,
  defaultZookeeperPort
}
import io.github.embeddedkafka.EmbeddedKafkaSpecSupport._
import org.scalatest.OptionValues

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

class EmbeddedKafkaObjectSpec
    extends EmbeddedKafkaSpecSupport
    with OptionValues {
  val consumerPollTimeout: FiniteDuration = 5.seconds

  "the EmbeddedKafka object" when {
    "invoking the start and stop methods" should {
      "start and stop Kafka and Zookeeper on the default ports" in {
        EmbeddedKafka.start()

        expectedServerStatus(defaultKafkaPort, Available)
        expectedServerStatus(defaultZookeeperPort, Available)

        EmbeddedKafka.stop()

        expectedServerStatus(defaultKafkaPort, NotAvailable)
        expectedServerStatus(defaultZookeeperPort, NotAvailable)
      }

      "start and stop Kafka and Zookeeper on different specified ports using an implicit configuration" in {
        implicit val config: EmbeddedKafkaConfig =
          EmbeddedKafkaConfig(kafkaPort = 12345, zooKeeperPort = 54321)
        EmbeddedKafka.start()

        expectedServerStatus(12345, Available)
        expectedServerStatus(54321, Available)

        EmbeddedKafka.stop()
      }

      "start and stop a specific Kafka" in {
        val firstBroker = EmbeddedKafka.start()(
          EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)
        )
        EmbeddedKafka.start()(
          EmbeddedKafkaConfig(kafkaPort = 8000, zooKeeperPort = 8001)
        )

        expectedServerStatus(7000, Available)
        expectedServerStatus(7001, Available)

        expectedServerStatus(8000, Available)
        expectedServerStatus(8001, Available)

        EmbeddedKafka.stop(firstBroker)

        expectedServerStatus(7000, NotAvailable)
        expectedServerStatus(7001, NotAvailable)

        expectedServerStatus(8000, Available)
        expectedServerStatus(8001, Available)

        EmbeddedKafka.stop()
      }

      "start and stop Kafka and Zookeeper successfully on arbitrary available ports" in {
        val someConfig =
          EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
        val kafka = EmbeddedKafka.start()(someConfig)

        kafka.factory shouldBe defined

        val usedZookeeperPort = EmbeddedKafka.zookeeperPort(kafka.factory.get)
        val usedKafkaPort     = EmbeddedKafka.kafkaPort(kafka.broker)

        expectedServerStatus(usedKafkaPort, Available)
        expectedServerStatus(usedZookeeperPort, Available)

        kafka.config.kafkaPort should be(usedKafkaPort)
        kafka.config.zooKeeperPort should be(usedZookeeperPort)

        EmbeddedKafka.stop()

        expectedServerStatus(usedKafkaPort, NotAvailable)
        expectedServerStatus(usedZookeeperPort, NotAvailable)
      }

      "start and stop multiple Kafka instances on specified ports" in {
        val someConfig =
          EmbeddedKafkaConfig(kafkaPort = 12345, zooKeeperPort = 32111)
        val someBroker = EmbeddedKafka.start()(someConfig)

        val someOtherConfig =
          EmbeddedKafkaConfig(kafkaPort = 23456, zooKeeperPort = 43211)
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
      "return true when both Kafka and Zookeeper are running" in {
        EmbeddedKafka.start()
        EmbeddedKafka.isRunning shouldBe true
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }

      "return true when both Kafka and Zookeeper are running, if started separately" in {
        EmbeddedKafka.startZooKeeper(
          Files.createTempDirectory("zookeeper-test-logs")
        )
        EmbeddedKafka.startKafka(Files.createTempDirectory("kafka-test-logs"))

        EmbeddedKafka.isRunning shouldBe true
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }

      "return false when only Zookeeper is running" in {
        EmbeddedKafka.startZooKeeper(
          Files.createTempDirectory("zookeeper-test-logs")
        )
        EmbeddedKafka.isRunning shouldBe false
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }
    }
  }
}
