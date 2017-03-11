package net.manub.embeddedkafka

import net.manub.embeddedkafka.ConsumerExtensions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class ConsumerOpsSpec extends EmbeddedKafkaSpecSupport with MockitoSugar {

  "ConsumeLazily " should {
    "retry to get messages with the configured maximum number of attempts when poll fails" in {
      val consumer = mock[KafkaConsumer[String, String]]
      val consumerRecords =
        new ConsumerRecords[String, String](Map.empty[TopicPartition, java.util.List[ConsumerRecord[String, String]]].asJava)

      val pollTimeout = 1
      when(consumer.poll(pollTimeout)).thenReturn(consumerRecords)

      val maximumAttempts = 2
      consumer.consumeLazily("topic", maximumAttempts, pollTimeout)

      verify(consumer, times(maximumAttempts)).poll(pollTimeout)
    }

    "not retry to get messages with the configured maximum number of attempts when poll succeeds" in {
      val consumer = mock[KafkaConsumer[String, String]]
      val consumerRecord = mock[ConsumerRecord[String, String]]
      val consumerRecords = new ConsumerRecords[String, String](
        Map[TopicPartition, java.util.List[ConsumerRecord[String, String]]](new TopicPartition("topic", 1) -> List(consumerRecord).asJava).asJava
      )

      val pollTimeout = 1
      when(consumer.poll(pollTimeout)).thenReturn(consumerRecords)

      val maximumAttempts = 2
      consumer.consumeLazily("topic", maximumAttempts, pollTimeout)

      verify(consumer).poll(pollTimeout)
    }

    "poll to get messages with the configured poll timeout" in {
      val consumer = mock[KafkaConsumer[String, String]]
      val consumerRecords =
        new ConsumerRecords[String, String](Map.empty[TopicPartition, java.util.List[ConsumerRecord[String, String]]].asJava)

      val pollTimeout = 10
      when(consumer.poll(pollTimeout)).thenReturn(consumerRecords)

      val maximumAttempts = 1
      consumer.consumeLazily("topic", maximumAttempts, pollTimeout)

      verify(consumer).poll(pollTimeout)
    }
  }

}
