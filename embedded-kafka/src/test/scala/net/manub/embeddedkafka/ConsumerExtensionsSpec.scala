package net.manub.embeddedkafka

import net.manub.embeddedkafka.Codecs.stringValueCrDecoder
import net.manub.embeddedkafka.ConsumerExtensions._
import org.apache.kafka.clients.consumer.{
  ConsumerRecord,
  ConsumerRecords,
  KafkaConsumer
}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.{times, verify, when}
import org.scalatestplus.mockito.MockitoSugar

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

class ConsumerExtensionsSpec
    extends EmbeddedKafkaSpecSupport
    with MockitoSugar {

  "consumeLazily" should {
    "retry to get messages with the configured maximum number of attempts when poll fails" in {
      implicit val retryConf: ConsumerRetryConfig =
        ConsumerRetryConfig(2, 1.millis)

      val consumer = mock[KafkaConsumer[String, String]]
      val consumerRecords =
        new ConsumerRecords[String, String](
          Map
            .empty[TopicPartition, java.util.List[
              ConsumerRecord[String, String]
            ]]
            .asJava
        )

      when(consumer.poll(duration2JavaDuration(retryConf.poll)))
        .thenReturn(consumerRecords)

      consumer.consumeLazily[String]("topic")

      verify(consumer, times(retryConf.maximumAttempts))
        .poll(duration2JavaDuration(retryConf.poll))
    }

    "not retry to get messages with the configured maximum number of attempts when poll succeeds" in {
      implicit val retryConf: ConsumerRetryConfig =
        ConsumerRetryConfig(2, 1.millis)

      val consumer       = mock[KafkaConsumer[String, String]]
      val consumerRecord = mock[ConsumerRecord[String, String]]
      val consumerRecords = new ConsumerRecords[String, String](
        Map[TopicPartition, java.util.List[ConsumerRecord[String, String]]](
          new TopicPartition("topic", 1) -> List(consumerRecord).asJava
        ).asJava
      )

      when(consumer.poll(duration2JavaDuration(retryConf.poll)))
        .thenReturn(consumerRecords)

      consumer.consumeLazily[String]("topic")

      verify(consumer).poll(duration2JavaDuration(retryConf.poll))
    }

    "poll to get messages with the configured poll timeout" in {
      implicit val retryConf: ConsumerRetryConfig =
        ConsumerRetryConfig(1, 10.millis)

      val consumer = mock[KafkaConsumer[String, String]]
      val consumerRecords =
        new ConsumerRecords[String, String](
          Map
            .empty[TopicPartition, java.util.List[
              ConsumerRecord[String, String]
            ]]
            .asJava
        )

      when(consumer.poll(duration2JavaDuration(retryConf.poll)))
        .thenReturn(consumerRecords)

      consumer.consumeLazily[String]("topic")

      verify(consumer).poll(duration2JavaDuration(retryConf.poll))
    }
  }
}
