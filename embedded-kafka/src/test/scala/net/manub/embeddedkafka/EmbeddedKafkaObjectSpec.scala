package net.manub.embeddedkafka

import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}
import net.manub.embeddedkafka.EmbeddedKafka._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.JavaConverters._
import scala.reflect.io.Directory

class EmbeddedKafkaObjectSpec extends EmbeddedKafkaSpecSupport {

  val consumerPollTimeout = 5000

  "the EmbeddedKafka object" when {
    "invoking the start and stop methods" should {
      "start and stop Kafka and Zookeeper on the default ports" in {
        EmbeddedKafka.start()

        kafkaIsAvailable()
        zookeeperIsAvailable()

        EmbeddedKafka.stop()

        kafkaIsNotAvailable()
        zookeeperIsNotAvailable()
      }

      "start and stop Kafka and Zookeeper on different specified ports using an implicit configuration" in {
        implicit val config: EmbeddedKafkaConfig =
          EmbeddedKafkaConfig(kafkaPort = 12345, zooKeeperPort = 54321)
        EmbeddedKafka.start()

        kafkaIsAvailable(12345)
        zookeeperIsAvailable(54321)

        EmbeddedKafka.stop()
      }

      "start and stop a specific Kafka" in {
        val firstBroker = EmbeddedKafka.start()(
          EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001))
        EmbeddedKafka.start()(
          EmbeddedKafkaConfig(kafkaPort = 8000, zooKeeperPort = 8001))

        kafkaIsAvailable(7000)
        zookeeperIsAvailable(7001)

        kafkaIsAvailable(8000)
        zookeeperIsAvailable(8001)

        EmbeddedKafka.stop(firstBroker)

        kafkaIsNotAvailable(7000)
        zookeeperIsNotAvailable(7001)

        kafkaIsAvailable(8000)
        zookeeperIsAvailable(8001)

        EmbeddedKafka.stop()
      }

      "start and stop Kafka and Zookeeper successfully on arbitrary available ports" in {
        val someConfig =
          EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
        val kafka = EmbeddedKafka.start()(someConfig)

        kafka.factory shouldBe defined

        val usedZookeeperPort = EmbeddedKafka.zookeeperPort(kafka.factory.get)
        val usedKafkaPort = EmbeddedKafka.kafkaPort(kafka.broker)

        kafkaIsAvailable(usedKafkaPort)
        zookeeperIsAvailable(usedZookeeperPort)

        kafka.config.kafkaPort should be(usedKafkaPort)
        kafka.config.zooKeeperPort should be(usedZookeeperPort)

        EmbeddedKafka.stop()

        kafkaIsNotAvailable(usedKafkaPort)
        zookeeperIsNotAvailable(usedZookeeperPort)
      }

      "start and stop multiple Kafka instances on specified ports" in {
        val someConfig =
          EmbeddedKafkaConfig(kafkaPort = 12345, zooKeeperPort = 32111)
        val someBroker = EmbeddedKafka.start()(someConfig)

        val someOtherConfig =
          EmbeddedKafkaConfig(kafkaPort = 23456, zooKeeperPort = 43211)
        val someOtherBroker = EmbeddedKafka.start()(someOtherConfig)

        val topic = "publish_test_topic_1"
        val someOtherMessage = "another message!"

        val serializer = new StringSerializer
        val deserializer = new StringDeserializer

        publishToKafka(topic, "hello world!")(someConfig, serializer)
        publishToKafka(topic, someOtherMessage)(someOtherConfig, serializer)

        kafkaIsAvailable(someConfig.kafkaPort)
        EmbeddedKafka.stop(someBroker)

        val anotherConsumer =
          kafkaConsumer(someOtherConfig, deserializer, deserializer)
        anotherConsumer.subscribe(List(topic).asJava)

        val moreRecords =
          anotherConsumer.poll(java.time.Duration.ofMillis(consumerPollTimeout))
        moreRecords.count shouldBe 1

        val someOtherRecord = moreRecords.iterator().next
        someOtherRecord.value shouldBe someOtherMessage

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

      "return false when only Kafka is running" in {
        val unmanagedZookeeper = EmbeddedKafka.startZooKeeper(
          6000,
          Directory.makeTemp("zookeeper-test-logs"))

        EmbeddedKafka.startKafka(Directory.makeTemp("kafka-test-logs"))
        EmbeddedKafka.isRunning shouldBe false
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false

        unmanagedZookeeper.shutdown()
      }

      "return false when only Zookeeper is running" in {
        EmbeddedKafka.startZooKeeper(Directory.makeTemp("zookeeper-test-logs"))
        EmbeddedKafka.isRunning shouldBe false
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }
    }
  }
}
