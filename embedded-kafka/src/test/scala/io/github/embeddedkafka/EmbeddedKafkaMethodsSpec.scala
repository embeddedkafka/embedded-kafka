package io.github.embeddedkafka

import io.github.embeddedkafka.EmbeddedKafka._
import io.github.embeddedkafka.serializers.{
  TestJsonDeserializer,
  TestJsonSerializer
}
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization._
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.scalatest.concurrent.JavaFutures
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{Assertion, BeforeAndAfterAll, OptionValues}

import java.util.Collections
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class EmbeddedKafkaMethodsSpec
    extends EmbeddedKafkaSpecSupport
    with BeforeAndAfterAll
    with OptionValues
    with JavaFutures {
  private val consumerPollTimeout: FiniteDuration = 5.seconds
  private implicit val patience: PatienceConfig   =
    PatienceConfig(Span(5, Seconds), Span(100, Milliseconds))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    super.afterAll()
  }

  "the publishToKafka method" should {
    "publish synchronously a String message to Kafka" in {
      implicit val serializer: StringSerializer     = new StringSerializer()
      implicit val deserializer: StringDeserializer = new StringDeserializer()
      val message                                   = "hello world!"
      val topic                                     = "publish_test_topic"

      publishToKafka(topic, message)

      withConsumer[String, String, Assertion] { consumer =>
        consumer.subscribe(List(topic).asJava)

        val records =
          consumer.poll(duration2JavaDuration(consumerPollTimeout))

        records.iterator().hasNext shouldBe true
        val record = records.iterator().next()

        record.value() shouldBe message
      }
    }

    "publish synchronously a String message with a header to Kafka" in {
      implicit val serializer: StringSerializer     = new StringSerializer()
      implicit val deserializer: StringDeserializer = new StringDeserializer()
      val message                                   = "hello world!"
      val topic       = "publish_test_topic_with_header"
      val headerValue = "my_header_value"
      val headers     = new RecordHeaders()
        .add("my_header", headerValue.toCharArray.map(_.toByte))
      val producerRecord =
        new ProducerRecord[String, String](topic, null, "key", message, headers)

      publishToKafka(producerRecord)

      withConsumer[String, String, Assertion] { consumer =>
        consumer.subscribe(List(topic).asJava)

        val records =
          consumer.poll(duration2JavaDuration(consumerPollTimeout))

        records.iterator().hasNext shouldBe true
        val record = records.iterator().next()

        record.value() shouldBe message
        val myHeader          = record.headers().lastHeader("my_header")
        val actualHeaderValue = new String(myHeader.value().map(_.toChar))
        actualHeaderValue shouldBe headerValue
      }
    }

    "publish synchronously a String message with String key to Kafka" in {
      implicit val serializer: StringSerializer     = new StringSerializer()
      implicit val deserializer: StringDeserializer = new StringDeserializer()
      val key                                       = "key"
      val message                                   = "hello world!"
      val topic = "publish_test_topic_string_key"

      publishToKafka(topic, key, message)

      withConsumer[String, String, Assertion] { consumer =>
        consumer.subscribe(List(topic).asJava)

        val records =
          consumer.poll(duration2JavaDuration(consumerPollTimeout))

        records.iterator().hasNext shouldBe true
        val record = records.iterator().next()

        record.key() shouldBe key
        record.value() shouldBe message
      }
    }

    "publish synchronously a batch of String messages with String keys to Kafka" in {
      implicit val serializer: StringSerializer     = new StringSerializer()
      implicit val deserializer: StringDeserializer = new StringDeserializer()
      val key1                                      = "key1"
      val message1                                  = "hello world!"
      val key2                                      = "key2"
      val message2                                  = "goodbye world!"
      val topic = "publish_test_topic_batch_string_key"

      val messages = List((key1, message1), (key2, message2))

      publishToKafka(topic, messages)

      withConsumer[String, String, Assertion] { consumer =>
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
      }
    }
  }

  "the createCustomTopic method" should {
    "create a topic with a custom configuration" in {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
        customBrokerProperties = Map(
          CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP -> 2000000.toString
        )
      )
      val topic = "test_custom_topic"

      createCustomTopic(
        topic,
        Map(
          TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT
        )
      )

      listTopics().getOrElse(Set.empty) should contain(topic)
    }

    "create a topic with custom number of partitions" in {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
      val topic = "test_custom_topic_with_custom_partitions"

      createCustomTopic(
        topic,
        Map(
          TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT
        ),
        partitions = 2
      )

      describeTopics(Seq(topic))
        .getOrElse(Map.empty)
        .apply(topic)
        .partitions()
        .size() shouldBe 2
    }
  }

  "the deleteTopics method" should {
    "either delete of mark for deletion a list of topics" in {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
        customBrokerProperties = Map(
          CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP -> 2000000.toString
        )
      )
      val topics = List("test_topic_deletion_1", "test_topic_deletion_2")

      topics.foreach(createCustomTopic(_))

      deleteTopics(topics)

      eventually {
        val allTopicsOrFailure = listTopics()
        assert(allTopicsOrFailure.isSuccess)

        val allTopics            = allTopicsOrFailure.getOrElse(Set.empty)
        val noTopicExistsAnymore = topics.forall(t => !allTopics.contains(t))
        assert(noTopicExistsAnymore)
      }
    }
  }

  "the consumeFirstStringMessageFrom method" should {
    val config = EmbeddedKafkaConfig()

    "return a message published to a topic" in {
      val message = "hello world!"
      val topic   = "consume_test_topic"

      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName
        ).asJava
      )

      whenReady(
        producer.send(new ProducerRecord[String, String](topic, message))
      ) { _ => consumeFirstStringMessageFrom(topic) shouldBe message }

      producer.close()
    }

    "consume only a single message when multiple messages have been published to a topic" in {
      val messages = Set("message 1", "message 2", "message 3")
      val topic    = "consume_test_topic"

      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName
        ).asJava
      )

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
      val topic   = "consume_test_topic"

      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName
        ).asJava
      )

      implicit val deser: Deserializer[Array[Byte]] = Codecs.nullDeserializer

      whenReady(
        producer.send(new ProducerRecord[String, String](topic, message))
      ) { _ =>
        consumeFirstMessageFrom[Array[Byte]](topic) shouldBe message.getBytes
      }

      producer.close()
    }

    "return a message published to a topic with custom decoder" in {
      val message                                        = TestClass("name")
      val topic                                          = "consume_test_topic"
      implicit val deserializer: Deserializer[TestClass] =
        new TestJsonDeserializer[TestClass]

      val producer = new KafkaProducer[String, TestClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        new StringSerializer,
        new TestJsonSerializer[TestClass]
      )

      whenReady(producer.send(new ProducerRecord(topic, message))) { _ =>
        consumeFirstMessageFrom[TestClass](topic) shouldBe message
      }

      producer.close()
    }
  }

  "the consumeFirstKeyedMessageFrom method" should {
    val config = EmbeddedKafkaConfig()

    "return a message published to a topic with implicit decoders" in {
      val key     = "greeting"
      val message = "hello world!"
      val topic   = "consume_test_topic"

      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName
        ).asJava
      )

      import Codecs._
      whenReady(
        producer
          .send(new ProducerRecord[String, String](topic, key, message))
      ) { _ =>
        val (k, v) =
          consumeFirstKeyedMessageFrom[Array[Byte], Array[Byte]](topic)
        k shouldBe key.getBytes
        v shouldBe message.getBytes
      }

      producer.close()
    }

    "return a message published to a topic with custom decoders" in {
      val key                                            = TestClass("key")
      val message                                        = TestClass("message")
      val topic                                          = "consume_test_topic"
      implicit val deserializer: Deserializer[TestClass] =
        new TestJsonDeserializer[TestClass]
      val serializer = new TestJsonSerializer[TestClass]

      val producer = new KafkaProducer[TestClass, TestClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        serializer,
        serializer
      )

      whenReady(producer.send(new ProducerRecord(topic, key, message))) { _ =>
        consumeFirstKeyedMessageFrom[TestClass, TestClass](
          topic
        ) shouldBe key -> message
      }

      producer.close()
    }

    "return a message published to a topic with 2 different decoders" in {
      val key                                             = "key"
      val message                                         = TestClass("message")
      val topic                                           = "consume_test_topic"
      implicit val stringDeserializer: StringDeserializer =
        new StringDeserializer
      implicit val deserializer: Deserializer[TestClass] =
        new TestJsonDeserializer[TestClass]

      val producer = new KafkaProducer[String, TestClass](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}"
        ).asJava,
        new StringSerializer,
        new TestJsonSerializer[TestClass]
      )

      whenReady(producer.send(new ProducerRecord(topic, key, message))) { _ =>
        consumeFirstKeyedMessageFrom[String, TestClass](
          topic
        ) shouldBe key -> message
      }

      producer.close()
    }
  }

  "the consumeNumberStringMessagesFrom method" should {
    val config = EmbeddedKafkaConfig()

    "consume set number of messages when multiple messages have been published to a topic" in {
      val messages = Set("message 1", "message 2", "message 3")
      val topic    = "consume_test_topic"
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName
        ).asJava
      )

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
      val topic    = "consume_test_topic"
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName
        ).asJava
      )

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
      val config           = EmbeddedKafkaConfig()
      val topicMessagesMap = Map(
        "topic1" -> List("message 1"),
        "topic2" -> List("message 2a", "message 2b")
      )
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName
        ).asJava
      )
      for ((topic, messages) <- topicMessagesMap; message <- messages) {
        producer.send(new ProducerRecord[String, String](topic, message))
      }

      producer.flush()

      implicit val deserializer: StringDeserializer = new StringDeserializer
      val consumedMessages                          =
        consumeNumberMessagesFromTopics(
          topicMessagesMap.keySet,
          topicMessagesMap.values.map(_.size).sum
        )

      consumedMessages.view
        .mapValues(_.sorted)
        .toMap shouldEqual topicMessagesMap

      producer.close()
    }
  }

  "the consumeNumberKeyedMessagesFromTopics method" should {
    "consume from multiple topics" in {
      val config           = EmbeddedKafkaConfig()
      val topicMessagesMap =
        Map(
          "topic1" -> List(("m1", "message 1")),
          "topic2" -> List(("m2a", "message 2a"), ("m2b", "message 2b"))
        )
      val producer = new KafkaProducer[String, String](
        Map[String, Object](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[
            StringSerializer
          ].getName
        ).asJava
      )
      for ((topic, messages) <- topicMessagesMap; (k, v) <- messages) {
        producer.send(new ProducerRecord[String, String](topic, k, v))
      }

      producer.flush()

      implicit val deserializer: StringDeserializer = new StringDeserializer
      val consumedMessages                          =
        consumeNumberKeyedMessagesFromTopics(
          topicMessagesMap.keySet,
          topicMessagesMap.values.map(_.size).sum
        )

      consumedMessages.view
        .mapValues(_.sorted)
        .toMap shouldEqual topicMessagesMap

      producer.close()
    }
  }

  "the consumer and producer loaner methods" should {
    "loan consumer and producer" in {
      implicit val serializer: Serializer[String]     = new StringSerializer()
      implicit val deserializer: Deserializer[String] = new StringDeserializer()
      val key                                         = "key"
      val value                                       = "value"
      val topic                                       = "loan_test_topic"

      withProducer[String, String, Unit] { producer =>
        val _ =
          producer.send(new ProducerRecord[String, String](topic, key, value))
      }

      withConsumer[String, String, Assertion](consumer => {
        consumer.subscribe(Collections.singletonList(topic))

        eventually {
          val records =
            consumer.poll(duration2JavaDuration(1.seconds)).asScala.toList
          records should have size 1
          val r :: _ = records
          r.key shouldBe key
          r.value shouldBe value
        }
      })
    }
  }
}

final case class TestClass(name: String)
