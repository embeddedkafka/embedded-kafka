package net.manub.embeddedkafka

import net.manub.embeddedkafka.ConsumerExtensions._
import org.apache.kafka.clients.consumer.{
  ConsumerRecord,
  ConsumerRecords,
  KafkaConsumer
}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class ConsumerExtensionsSpec
    extends EmbeddedKafkaSpecSupport
    with MockitoSugar {

  import net.manub.embeddedkafka.Codecs.stringValueCrDecoder

  "consumeLazily" should {

    "retry to get messages with the configured maximum number of attempts when poll fails" in {

      implicit val retryConf = ConsumerRetryConfig(2, 1)

      val consumer = mock[KafkaConsumer[String, String]]
      val consumerRecords =
        new ConsumerRecords[String, String](Map
          .empty[TopicPartition, java.util.List[ConsumerRecord[String, String]]]
          .asJava)

      when(consumer.poll(retryConf.poll)).thenReturn(consumerRecords)

      consumer.consumeLazily[String]("topic")

      verify(consumer, times(retryConf.maximumAttempts)).poll(retryConf.poll)
    }

    "not retry to get messages with the configured maximum number of attempts when poll succeeds" in {

      implicit val retryConf = ConsumerRetryConfig(2, 1)

      val consumer = mock[KafkaConsumer[String, String]]
      val consumerRecord = mock[ConsumerRecord[String, String]]
      val consumerRecords = new ConsumerRecords[String, String](
        Map[TopicPartition, java.util.List[ConsumerRecord[String, String]]](
          new TopicPartition("topic", 1) -> List(consumerRecord).asJava).asJava
      )

      when(consumer.poll(retryConf.poll)).thenReturn(consumerRecords)

      consumer.consumeLazily[String]("topic")

      verify(consumer).poll(retryConf.poll)
    }

    "poll to get messages with the configured poll timeout" in {

      implicit val retryConf = ConsumerRetryConfig(1, 10)

      val consumer = mock[KafkaConsumer[String, String]]
      val consumerRecords =
        new ConsumerRecords[String, String](Map
          .empty[TopicPartition, java.util.List[ConsumerRecord[String, String]]]
          .asJava)

      when(consumer.poll(retryConf.poll)).thenReturn(consumerRecords)

      consumer.consumeLazily[String]("topic")

      verify(consumer).poll(retryConf.poll)
    }
  }

}
