package net.manub.embeddedkafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.KafkaException
import org.apache.log4j.Logger

import scala.util.Try

/** Method extensions for Kafka's [[KafkaConsumer]] API allowing easy testing.*/
object ConsumerExtensions {
  val MaximumAttempts = 3
  implicit class ConsumerOps[K, V](val consumer: KafkaConsumer[K, V]) {

    private val logger = Logger.getLogger(classOf[ConsumerOps[K, V]])

    /** Consume messages from a given topic and return them as a lazily evaluated Scala Stream.
      * Depending on how many messages are taken from the Scala Stream it will try up to 3 times
      * to consume batches from the given topic, until it reaches the number of desired messages or
      * return otherwise.
      *
      * @param topic the topic from which to consume messages
      * @return the stream of consumed messages that you can do `.take(n: Int).toList`
      *         to evaluate the requested number of messages.
      */
    def consumeLazily(topic: String): Stream[(K, V)] = {
      val attempts = 1 to MaximumAttempts
      attempts.toStream.flatMap { attempt =>
        val batch: Seq[(K, V)] = getNextBatch(topic)
        logger.debug(s"----> Batch $attempt ($topic) | ${batch.mkString("|")}")
        batch
      }
    }

    /** Get the next batch of messages from Kafka.
      *
      * @param topic the topic to consume
      * @return the next batch of messages
      */
    def getNextBatch(topic: String): Seq[(K, V)] =
      Try {
        import scala.collection.JavaConversions._
        consumer.subscribe(List(topic))
        consumer.partitionsFor(topic)
        val records = consumer.poll(2000)
        // use toList to force eager evaluation. toSeq is lazy
        records.iterator().toList.map(r => r.key -> r.value)
      }.recover {
        case ex: KafkaException => throw new KafkaUnavailableException(ex)
      }.get
  }
}
