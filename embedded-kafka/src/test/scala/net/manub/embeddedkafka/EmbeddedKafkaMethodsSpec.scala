package net.manub.embeddedkafka

import java.util.concurrent.TimeoutException

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.serialization.{
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

class EmbeddedKafkaMethodsSpec
    extends EmbeddedKafkaSpecSupport
    with EmbeddedKafka
    with BeforeAndAfterAll {

  val consumerPollTimeout = 5000

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    super.afterAll()
  }

  "the publishToKafka method" should {
    "publish synchronously a String message to Kafka" in {
      implicit val serializer = new StringSerializer()
      implicit val deserializer = new StringDeserializer()
      val message = "hello world!"
      val topic = "publish_test_topic"

      publishToKafka(topic, message)

      val consumer = kafkaConsumer
      consumer.subscribe(List(topic).asJava)

      val records = consumer.poll(consumerPollTimeout)

      records.iterator().hasNext shouldBe true
      val record = records.iterator().next()

      record.value() shouldBe message

      consumer.close()
    }

    "publish synchronously a String message with String key to Kafka" in {
      implicit val serializer = new StringSerializer()
      implicit val deserializer = new StringDeserializer()
      val key = "key"
      val message = "hello world!"
      val topic = "publish_test_topic_string_key"

      publishToKafka(topic, key, message)

      val consumer = kafkaConsumer
      consumer.subscribe(List(topic).asJava)

      val records = consumer.poll(consumerPollTimeout)

      records.iterator().hasNext shouldBe true
      val record = records.iterator().next()

      record.key() shouldBe key
      record.value() shouldBe message

      consumer.close()
    }
  }

  "the createCustomTopic method" should {
    "create a topic with a custom configuration" in {
      implicit val config = EmbeddedKafkaConfig(
        customBrokerProperties =
          Map("log.cleaner.dedupe.buffer.size" -> "2000000"))
      val topic = "test_custom_topic"

      createCustomTopic(topic, Map("cleanup.policy" -> "compact"))

      val zkSessionTimeoutMs = 10000
      val zkConnectionTimeoutMs = 10000
      val zkSecurityEnabled = false

      val zkUtils = ZkUtils(s"localhost:${config.zooKeeperPort}",
                            zkSessionTimeoutMs,
                            zkConnectionTimeoutMs,
                            zkSecurityEnabled)
      try {
        AdminUtils.topicExists(zkUtils, topic) shouldBe true
      } finally zkUtils.close()

    }

    "create a topic with custom number of partitions" in {
      implicit val config = EmbeddedKafkaConfig()
      val topic = "test_custom_topic_with_custom_partitions"

      createCustomTopic(topic,
                        Map("cleanup.policy" -> "compact"),
                        partitions = 2)

      val zkSessionTimeoutMs = 10000
      val zkConnectionTimeoutMs = 10000
      val zkSecurityEnabled = false

      val zkUtils = ZkUtils(s"localhost:${config.zooKeeperPort}",
                            zkSessionTimeoutMs,
                            zkConnectionTimeoutMs,
                            zkSecurityEnabled)
      try {
        AdminUtils
          .fetchTopicMetadataFromZk(topic, zkUtils)
          .partitionMetadata()
          .size shouldBe 2
      } finally zkUtils.close()

    }
  }

  "the consumeFirstStringMessageFrom method" should {
    val config = EmbeddedKafkaConfig()

    "return a message published to a topic" in {
      val message = "hello world!"
      val topic = "consume_test_topic"

      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)

      whenReady(
        producer.send(new ProducerRecord[String, String](topic, message))) {
        _ =>
          consumeFirstStringMessageFrom(topic) shouldBe message
      }

      producer.close()
    }

    "consume only a single message when multiple messages have been published to a topic" in {
      val messages = Set("message 1", "message 2", "message 3")
      val topic = "consume_test_topic"

      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)

      messages.foreach { message =>
        producer.send(new ProducerRecord[String, String](topic, message))
      }

      producer.flush()

      val consumedMessages = for (_ <- 1 to messages.size) yield {
        consumeFirstStringMessageFrom(topic)
      }

      consumedMessages.toSet shouldEqual messages

      producer.close()
    }

    "throw a TimeoutExeption when a message is not available" in {
      a[TimeoutException] shouldBe thrownBy {
        consumeFirstStringMessageFrom("non_existing_topic")
      }
    }
  }

  "the consumeFirstMessageFrom method" should {
    val config = EmbeddedKafkaConfig()

    "return a message published to a topic with implicit decoder" in {
      val message = "hello world!"
      val topic = "consume_test_topic"

      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)

      import Codecs._
      whenReady(
        producer.send(new ProducerRecord[String, String](topic, message))) {
        _ =>
          consumeFirstMessageFrom[Array[Byte]](topic) shouldBe message.getBytes
      }

      producer.close()
    }

    "return a message published to a topic with custom decoder" in {

      import avro._

      val message = TestAvroClass("name")
      val topic = "consume_test_topic"
      implicit val testAvroClassDecoder =
        specificAvroDeserializer[TestAvroClass](TestAvroClass.SCHEMA$)

      val producer = new KafkaProducer[String, TestAvroClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        new StringSerializer,
        specificAvroSerializer[TestAvroClass])

      whenReady(producer.send(new ProducerRecord(topic, message))) { _ =>
        consumeFirstMessageFrom[TestAvroClass](topic) shouldBe message
      }

      producer.close()
    }
  }

  "the consumeFirstKeyedMessageFrom method" should {
    val config = EmbeddedKafkaConfig()

    "return a message published to a topic with implicit decoders" in {
      val key = "greeting"
      val message = "hello world!"
      val topic = "consume_test_topic"

      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)

      import Codecs._
      whenReady(
        producer.send(new ProducerRecord[String, String](topic, key, message))) {
        _ =>
          val res = consumeFirstKeyedMessageFrom[Array[Byte], Array[Byte]](topic)
          res._1 shouldBe key.getBytes
          res._2 shouldBe message.getBytes
      }

      producer.close()
    }

    "return a message published to a topic with custom decoders" in {

      import avro._

      val key = TestAvroClass("key")
      val message = TestAvroClass("message")
      val topic = "consume_test_topic"
      implicit val testAvroClassDecoder =
        specificAvroDeserializer[TestAvroClass](TestAvroClass.SCHEMA$)

      val producer = new KafkaProducer[TestAvroClass, TestAvroClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        specificAvroSerializer[TestAvroClass],
        specificAvroSerializer[TestAvroClass])

      whenReady(producer.send(new ProducerRecord(topic, key, message))) { _ =>
        consumeFirstKeyedMessageFrom[TestAvroClass, TestAvroClass](topic) shouldBe (key, message)
      }

      producer.close()
    }

    "return a message published to a topic with 2 different decoders" in {

      import avro._

      val key = "key"
      val message = TestAvroClass("message")
      val topic = "consume_test_topic"
      implicit val stringDecoder = new StringDeserializer
      implicit val testAvroClassDecoder =
        specificAvroDeserializer[TestAvroClass](TestAvroClass.SCHEMA$)

      val producer = new KafkaProducer[String, TestAvroClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        new StringSerializer,
        specificAvroSerializer[TestAvroClass])

      whenReady(producer.send(new ProducerRecord(topic, key, message))) { _ =>
        consumeFirstKeyedMessageFrom[String, TestAvroClass](topic) shouldBe (key, message)
      }

      producer.close()
    }
  }

  "the consumeNumberStringMessagesFrom method" should {
    val config = EmbeddedKafkaConfig()

    "consume set number of messages when multiple messages have been published to a topic" in {
      val messages = Set("message 1", "message 2", "message 3")
      val topic = "consume_test_topic"
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)

      messages.foreach { message =>
        producer.send(new ProducerRecord[String, String](topic, message))
      }

      producer.flush()

      val consumedMessages =
        consumeNumberStringMessagesFrom(topic, messages.size)

      consumedMessages.toSet shouldEqual messages

      producer.close()
    }

    "timeout and throw a TimeoutException when n messages are not received in time" in {
      val messages = Set("message 1", "message 2", "message 3")
      val topic = "consume_test_topic"
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)

      messages.foreach { message =>
        producer.send(new ProducerRecord[String, String](topic, message))
      }

      producer.flush()

      a[TimeoutException] shouldBe thrownBy {
        consumeNumberStringMessagesFrom(topic, messages.size + 1)
      }

      producer.close()
    }
  }

  "the consumeNumberMessagesFromTopics method" should {
    "consume from multiple topics" in {
      val config = EmbeddedKafkaConfig()
      val topicMessagesMap = Map("topic1" -> List("message 1"), "topic2" -> List("message 2a", "message 2b"))
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)
      for ((topic, messages) <- topicMessagesMap; message <- messages) {
        producer.send(new ProducerRecord[String, String](topic, message))
      }

      producer.flush()

      implicit val deserializer = new StringDeserializer
      val consumedMessages =
        consumeNumberMessagesFromTopics(topicMessagesMap.keySet, topicMessagesMap.values.map(_.size).sum)

      consumedMessages.mapValues(_.sorted) shouldEqual topicMessagesMap

      producer.close()
    }
  }

  "the consumeNumberKeyedMessagesFromTopics method" should {
    "consume from multiple topics" in {
      val config = EmbeddedKafkaConfig()
      val topicMessagesMap = Map("topic1" -> List(("m1", "message 1")),
        "topic2" -> List(("m2a", "message 2a"), ("m2b", "message 2b")))
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)
      for ((topic, messages) <- topicMessagesMap; message <- messages) {
        producer.send(new ProducerRecord[String, String](topic, message._1, message._2))
      }

      producer.flush()

      implicit val deserializer = new StringDeserializer
      val consumedMessages =
        consumeNumberKeyedMessagesFromTopics(topicMessagesMap.keySet, topicMessagesMap.values.map(_.size).sum)

      consumedMessages.mapValues(_.sorted) shouldEqual topicMessagesMap

      producer.close()
    }
  }

  "the aKafkaProducerThat method" should {
    "return a producer that encodes messages for the given encoder" in {
      val producer = aKafkaProducer thatSerializesValuesWith classOf[
          ByteArraySerializer]
      producer.send(
        new ProducerRecord[String, Array[Byte]]("a_topic",
                                                "a message".getBytes))
      producer.close()
    }
  }

  "the aKafkaProducer object" should {
    "return a producer that encodes messages for the given type" in {
      import Codecs._
      val producer = aKafkaProducer[String]
      producer.send(new ProducerRecord[String, String]("a_topic", "a message"))
      producer.close()
    }

    "return a producer that encodes messages for a custom type" in {
      import avro._
      val producer = aKafkaProducer[TestAvroClass]
      producer.send(
        new ProducerRecord[String, TestAvroClass]("a_topic",
                                                  TestAvroClass("name")))
      producer.close()
    }
  }
}
