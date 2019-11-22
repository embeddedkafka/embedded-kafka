package net.manub.embeddedkafka

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.KafkaException

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.util.Try

/** Method extensions for Kafka's [[KafkaConsumer]] API allowing easy testing. */
object ConsumerExtensions {
  case class ConsumerRetryConfig(
      maximumAttempts: Int = 3,
      poll: FiniteDuration = 2.seconds
  )

  implicit class ConsumerOps[K, V](val consumer: KafkaConsumer[K, V]) {
    /** Consume messages from one or many topics and return them as a lazily evaluated Scala Stream.
      * Depending on how many messages are taken from the Scala Stream it will try up to retryConf.maximumAttempts times
      * to consume batches from the given topic, until it reaches the number of desired messages or
      * return otherwise.
      *
      * @param topics          the topics from which to consume messages
      * @param decoder         the function to use for decoding all [[ConsumerRecord]]
      * @param retryConf       contains the maximum number of attempts to try and get the next batch and the amount
      *                        of time, in milliseconds, to wait in the buffer for any messages to be available
      * @return the stream of consumed messages that you can do `.take(n: Int).toList`
      *         to evaluate the requested number of messages.
      */
    def consumeLazily[T](topics: String*)(
        implicit decoder: ConsumerRecord[K, V] => T,
        retryConf: ConsumerRetryConfig = ConsumerRetryConfig()
    ): Stream[T] = {
      val attempts = 1 to retryConf.maximumAttempts
      attempts.toStream.flatMap { _ =>
        val batch: Seq[T] = getNextBatch(retryConf.poll, topics)
        batch
      }
    }

    /** Get the next batch of messages from Kafka.
      *
      * @param poll            the amount of time to wait in the buffer for any messages to be available
      * @param topics          the topic to consume
      * @param decoder         the function to use for decoding all [[ConsumerRecord]]
      * @return the next batch of messages
      */
    private def getNextBatch[T](poll: FiniteDuration, topics: Seq[String])(
        implicit decoder: ConsumerRecord[K, V] => T
    ): Seq[T] =
      Try {
        consumer.subscribe(topics.asJava)
        topics.foreach(consumer.partitionsFor)
        val records = consumer.poll(duration2JavaDuration(poll))
        // use toList to force eager evaluation. toSeq is lazy
        records.iterator().asScala.toList.map(decoder(_))
      }.recover {
        case ex: KafkaException => throw new KafkaUnavailableException(ex)
      }.get
  }
}
