package net.manub.embeddedkafka

import java.util.concurrent.TimeoutException

import kafka.server.KafkaConfig
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.{
  ByteArraySerializer,
  Deserializer,
  StringDeserializer,
  StringSerializer
}
import org.apache.kafka.common.utils.Time
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.scalatest.OptionValues._

class EmbeddedKafkaMethodsSpec
    extends EmbeddedKafkaSpecSupport
    with EmbeddedKafka
    with BeforeAndAfterAll {

  val consumerPollTimeout: FiniteDuration = 5.seconds

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
      implicit val serializer: StringSerializer = new StringSerializer()
      implicit val deserializer: StringDeserializer = new StringDeserializer()
      val message = "hello world!"
      val topic = "publish_test_topic"

      publishToKafka(topic, message)

      val consumer = kafkaConsumer
      consumer.subscribe(List(topic).asJava)

      val records =
        consumer.poll(duration2JavaDuration(consumerPollTimeout))

      records.iterator().hasNext shouldBe true
      val record = records.iterator().next()

      record.value() shouldBe message

      consumer.close()
    }

    "publish synchronously a String message with a header to Kafka" in {
      implicit val serializer: StringSerializer = new StringSerializer()
      implicit val deserializer: StringDeserializer = new StringDeserializer()
      val message = "hello world!"
      val topic = "publish_test_topic_with_header"
      val headerValue = "my_header_value"
      val headers = new RecordHeaders()
        .add("my_header", headerValue.toCharArray.map(_.toByte))
      val producerRecord =
        new ProducerRecord[String, String](topic, null, "key", message, headers)

      publishToKafka(producerRecord)

      val consumer = kafkaConsumer
      consumer.subscribe(List(topic).asJava)

      val records =
        consumer.poll(duration2JavaDuration(consumerPollTimeout))

      records.iterator().hasNext shouldBe true
      val record = records.iterator().next()

      record.value() shouldBe message
      val myHeader = record.headers().lastHeader("my_header")
      val actualHeaderValue = new String(myHeader.value().map(_.toChar))
      actualHeaderValue shouldBe headerValue

      consumer.close()
    }

    "publish synchronously a String message with String key to Kafka" in {
      implicit val serializer: StringSerializer = new StringSerializer()
      implicit val deserializer: StringDeserializer = new StringDeserializer()
      val key = "key"
      val message = "hello world!"
      val topic = "publish_test_topic_string_key"

      publishToKafka(topic, key, message)

      val consumer = kafkaConsumer
      consumer.subscribe(List(topic).asJava)

      val records =
        consumer.poll(duration2JavaDuration(consumerPollTimeout))

      records.iterator().hasNext shouldBe true
      val record = records.iterator().next()

      record.key() shouldBe key
      record.value() shouldBe message

      consumer.close()
    }

    "publish synchronously a batch of String messages with String keys to Kafka" in {
      implicit val serializer: StringSerializer = new StringSerializer()
      implicit val deserializer: StringDeserializer = new StringDeserializer()
      val key1 = "key1"
      val message1 = "hello world!"
      val key2 = "key2"
      val message2 = "goodbye world!"
      val topic = "publish_test_topic_batch_string_key"

      val messages = List((key1, message1), (key2, message2))

      publishToKafka(topic, messages)

      val consumer = kafkaConsumer
      consumer.subscribe(List(topic).asJava)

      val records = consumer
        .poll(duration2JavaDuration(consumerPollTimeout))
        .iterator()

      records.hasNext shouldBe true

      val record1 = records.next()
      record1.key() shouldBe key1
      record1.value() shouldBe message1

      records.hasNext shouldBe true
      val record2 = records.next()
      record2.key() shouldBe key2
      record2.value() shouldBe message2

      consumer.close()
    }
  }

  "the createCustomTopic method" should {
    "create a topic with a custom configuration" in {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
        customBrokerProperties = Map(
          KafkaConfig.LogCleanerDedupeBufferSizeProp -> 2000000.toString
        ))
      val topic = "test_custom_topic"

      createCustomTopic(
        topic,
        Map(
          TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT))

      val zkClient = KafkaZkClient.apply(
        s"localhost:${config.zooKeeperPort}",
        isSecure = false,
        zkSessionTimeoutMs,
        zkConnectionTimeoutMs,
        maxInFlightRequests = 1,
        Time.SYSTEM
      )

      try {
        zkClient.topicExists(topic) shouldBe true
      } finally zkClient.close()
    }

    "create a topic with custom number of partitions" in {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
      val topic = "test_custom_topic_with_custom_partitions"

      createCustomTopic(
        topic,
        Map(
          TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT),
        partitions = 2)

      val zkClient = KafkaZkClient.apply(
        s"localhost:${config.zooKeeperPort}",
        isSecure = false,
        zkSessionTimeoutMs,
        zkConnectionTimeoutMs,
        maxInFlightRequests = 1,
        Time.SYSTEM
      )

      try {
        zkClient.getTopicPartitionCount(topic).value shouldBe 2
      } finally zkClient.close()
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
      implicit val testAvroClassDecoder: Deserializer[TestAvroClass] =
        specificAvroDeserializer[TestAvroClass](TestAvroClass.SCHEMA$)

      val producer = new KafkaProducer[String, TestAvroClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        new StringSerializer,
        specificAvroSerializer[TestAvroClass]
      )

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
        producer
          .send(new ProducerRecord[String, String](topic, key, message))) { _ =>
        val (k, v) =
          consumeFirstKeyedMessageFrom[Array[Byte], Array[Byte]](topic)
        k shouldBe key.getBytes
        v shouldBe message.getBytes
      }

      producer.close()
    }

    "return a message published to a topic with custom decoders" in {

      import avro._

      val key = TestAvroClass("key")
      val message = TestAvroClass("message")
      val topic = "consume_test_topic"
      implicit val testAvroClassDecoder: Deserializer[TestAvroClass] =
        specificAvroDeserializer[TestAvroClass](TestAvroClass.SCHEMA$)

      val producer = new KafkaProducer[TestAvroClass, TestAvroClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        specificAvroSerializer[TestAvroClass],
        specificAvroSerializer[TestAvroClass]
      )

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
      implicit val stringDecoder: StringDeserializer = new StringDeserializer
      implicit val testAvroClassDecoder: Deserializer[TestAvroClass] =
        specificAvroDeserializer[TestAvroClass](TestAvroClass.SCHEMA$)

      val producer = new KafkaProducer[String, TestAvroClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        new StringSerializer,
        specificAvroSerializer[TestAvroClass]
      )

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
      val topicMessagesMap = Map("topic1" -> List("message 1"),
                                 "topic2" -> List("message 2a", "message 2b"))
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

      implicit val deserializer: StringDeserializer = new StringDeserializer
      val consumedMessages =
        consumeNumberMessagesFromTopics(topicMessagesMap.keySet,
                                        topicMessagesMap.values.map(_.size).sum)

      consumedMessages.mapValues(_.sorted) shouldEqual topicMessagesMap

      producer.close()
    }
  }

  "the consumeNumberKeyedMessagesFromTopics method" should {
    "consume from multiple topics" in {
      val config = EmbeddedKafkaConfig()
      val topicMessagesMap =
        Map("topic1" -> List(("m1", "message 1")),
            "topic2" -> List(("m2a", "message 2a"), ("m2b", "message 2b")))
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer].getName
        ).asJava)
      for ((topic, messages) <- topicMessagesMap; (k, v) <- messages) {
        producer.send(new ProducerRecord[String, String](topic, k, v))
      }

      producer.flush()

      implicit val deserializer: StringDeserializer = new StringDeserializer
      val consumedMessages =
        consumeNumberKeyedMessagesFromTopics(
          topicMessagesMap.keySet,
          topicMessagesMap.values.map(_.size).sum)

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
