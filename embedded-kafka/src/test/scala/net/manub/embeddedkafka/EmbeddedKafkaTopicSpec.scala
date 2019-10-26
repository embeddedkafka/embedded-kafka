package net.manub.embeddedkafka

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.JavaConverters._

class EmbeddedKafkaTopicSpec extends FlatSpec with BeforeAndAfter {
  private val TopicName = "abc"
  private val MaxWaitingTime = 7L
  val properties = new Properties()
  properties.setProperty(
  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
  "localhost:6001"
  )
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testEK")
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  private val deserializer = new StringDeserializer()
  val consumerA =
    new KafkaConsumer[String, String](properties, deserializer, deserializer)

  val consumerB =
    new KafkaConsumer[String, String](properties, deserializer, deserializer)
  before {
    EmbeddedKafka.start()
  }

  after {
    EmbeddedKafka.stop()
  }

  behavior of "Embedded Kafka"

  it should "publish to topic" in {
    EmbeddedKafka.publishStringMessageToKafka(TopicName, "message")
    consumerA.subscribe(List(TopicName).asJava)
    consumerB.subscribe(List(TopicName).asJava)
    val res = consumerA.poll(Duration.ofSeconds(MaxWaitingTime))
    assert(!res.isEmpty)
  }

  it should "not persist the topic data after stop of the EmbeddedKafka" in {
    val resA = consumerA.poll(Duration.ofSeconds(MaxWaitingTime))
    assert(resA.isEmpty)
    val resB = consumerB.poll(Duration.ofSeconds(MaxWaitingTime))
    assert(resB.isEmpty)
  }

}
