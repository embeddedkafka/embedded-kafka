package net.manub.embeddedkafka.schemaregistry

import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.{EmbeddedKafkaSpecSupport, TestAvroClass}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._

class EmbeddedKafkaWithSchemaRegistrySpec
    extends EmbeddedKafkaSpecSupport
    with EmbeddedKafkaWithSchemaRegistry {

  implicit lazy val embeddedKafkaConfig: EmbeddedKafkaConfigWithSchemaRegistry =
    EmbeddedKafkaConfigWithSchemaRegistry()

  val consumerPollTimeout = 5000

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafkaWithSchemaRegistry.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafkaWithSchemaRegistry.stop()
    super.afterAll()
  }

  "the publishToKafka method" should {
    "publish synchronously a message to Kafka storing its schema into Schema Registry" in {
      val message = TestAvroClass("name")
      val topic = "publish_test_topic"
      publishToKafka(topic, message)

      val consumer = kafkaConsumer[String, TestAvroClass]
      consumer.subscribe(List(topic).asJava)

      val records = consumer.poll(consumerPollTimeout)

      records.iterator().hasNext shouldBe true
      val record = records.iterator().next()

      record.value() shouldBe message

      consumer.close()
    }

    "publish synchronously a message with String key to Kafka storing its schema into Schema Registry" in {
      val key = "key"
      val message = TestAvroClass("name")
      val topic = "publish_test_topic_string_key"

      publishToKafka(topic, key, message)

      val consumer = kafkaConsumer[String, TestAvroClass]
      consumer.subscribe(List(topic).asJava)

      val records = consumer.poll(consumerPollTimeout)

      records.iterator().hasNext shouldBe true
      val record = records.iterator().next()

      record.key() shouldBe key
      record.value() shouldBe message

      consumer.close()
    }

    "publish synchronously a batch of messages with String keys to Kafka storing its schema into Schema Registry" in {
      val key1 = "key1"
      val message1 = TestAvroClass("name")
      val key2 = "key2"
      val message2 = TestAvroClass("other name")
      val topic = "publish_test_topic_batch_string_key"

      val messages = List((key1, message1), (key2, message2))

      publishToKafka(topic, messages)

      val consumer = kafkaConsumer[String, TestAvroClass]
      consumer.subscribe(List(topic).asJava)

      val records = consumer.poll(consumerPollTimeout).iterator()

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

  "the consumeFirstMessageFrom method" should {
    "return a message published to a topic reading its schema from Schema Registry" in {
      val message = TestAvroClass("name")
      val topic = "consume_test_topic"

      val producer = aKafkaProducer[TestAvroClass]
      producer.send(new ProducerRecord[String, TestAvroClass](topic, message))

      consumeFirstMessageFrom[TestAvroClass](topic) shouldBe message

      producer.close()
    }
  }

  "the consumeFirstKeyedMessageFrom method" should {
    "return a message with String key published to a topic reading its schema from Schema Registry" in {
      val key = "greeting"
      val message = TestAvroClass("name")
      val topic = "consume_test_topic"

      val producer = aKafkaProducer[TestAvroClass]
      producer.send(
        new ProducerRecord[String, TestAvroClass](topic, key, message))

      val res = consumeFirstKeyedMessageFrom[String, TestAvroClass](topic)
      res._1 shouldBe key
      res._2 shouldBe message

      producer.close()
    }
  }

  "the consumeNumberMessagesFromTopics method" should {
    "consume from multiple topics reading messages schema from Schema Registry" in {
      val topicMessagesMap = Map("topic1" -> List(TestAvroClass("name")),
                                 "topic2" -> List(TestAvroClass("other name")))
      val producer = aKafkaProducer[TestAvroClass]
      for ((topic, messages) <- topicMessagesMap; message <- messages) {
        producer.send(new ProducerRecord[String, TestAvroClass](topic, message))
      }

      producer.flush()

      val consumedMessages =
        consumeNumberMessagesFromTopics[TestAvroClass](
          topicMessagesMap.keySet,
          topicMessagesMap.values.map(_.size).sum)

      consumedMessages shouldEqual topicMessagesMap

      producer.close()
    }
  }

  "the consumeNumberKeyedMessagesFromTopics method" should {
    "consume from multiple topics reading messages schema from Schema Registry" in {
      val topicMessagesMap =
        Map("topic1" -> List(("m1", TestAvroClass("name"))),
            "topic2" -> List(("m2a", TestAvroClass("other name")),
                             ("m2b", TestAvroClass("even another name"))))
      val producer = aKafkaProducer[TestAvroClass]
      for ((topic, messages) <- topicMessagesMap; message <- messages) {
        producer.send(
          new ProducerRecord[String, TestAvroClass](topic,
                                                    message._1,
                                                    message._2))
      }

      producer.flush()

      val consumedMessages =
        consumeNumberKeyedMessagesFromTopics[String, TestAvroClass](
          topicMessagesMap.keySet,
          topicMessagesMap.values.map(_.size).sum)

      consumedMessages shouldEqual topicMessagesMap

      producer.close()
    }
  }

}
