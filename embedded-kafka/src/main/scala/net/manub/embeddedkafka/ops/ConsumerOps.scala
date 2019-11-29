package net.manub.embeddedkafka.ops

import net.manub.embeddedkafka.{
  EmbeddedKafkaConfig,
  KafkaUnavailableException,
  duration2JavaDuration
}
import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  KafkaConsumer,
  OffsetAndMetadata,
  OffsetResetStrategy
}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.collection.mutable.ListBuffer
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.Try
import scala.collection.JavaConverters._

/**
  * Trait for Consumer-related actions.
  *
  * @tparam C an [[EmbeddedKafkaConfig]]
  */
trait ConsumerOps[C <: EmbeddedKafkaConfig] {
  protected val consumerPollingTimeout: FiniteDuration = 1.second

  private[embeddedkafka] def baseConsumerConfig(
      implicit config: C
  ): Map[String, Object]

  private[embeddedkafka] def defaultConsumerConfig(implicit config: C) =
    Map[String, Object](
      ConsumerConfig.GROUP_ID_CONFIG           -> "embedded-kafka-spec",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG  -> s"localhost:${config.kafkaPort}",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG  -> OffsetResetStrategy.EARLIEST.toString.toLowerCase,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> false.toString
    )

  def kafkaConsumer[K, T](
      implicit config: C,
      keyDeserializer: Deserializer[K],
      deserializer: Deserializer[T]
  ) =
    new KafkaConsumer[K, T](
      baseConsumerConfig.asJava,
      keyDeserializer,
      deserializer
    )

  def consumeFirstStringMessageFrom(topic: String, autoCommit: Boolean = false)(
      implicit config: C
  ): String =
    consumeNumberStringMessagesFrom(topic, 1, autoCommit)(config).head

  def consumeNumberStringMessagesFrom(
      topic: String,
      number: Int,
      autoCommit: Boolean = false
  )(implicit config: C): List[String] =
    consumeNumberMessagesFrom(topic, number, autoCommit)(
      config,
      new StringDeserializer()
    )

  /**
    * Consumes the first message available in a given topic, deserializing it as type [[V]].
    *
    * Only the message that is returned is committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topic        the topic to consume a message from
    * @param autoCommit   if false, only the offset for the consumed message will be committed.
    *                     if true, the offset for the last polled message will be committed instead.
    *                     Defaulted to false.
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param valueDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[V]]
    * @return the first message consumed from the given topic, with a type [[V]]
    * @throws TimeoutException          if unable to consume a message within 5 seconds
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstMessageFrom[V](
      topic: String,
      autoCommit: Boolean = false
  )(implicit config: C, valueDeserializer: Deserializer[V]): V =
    consumeNumberMessagesFrom[V](topic, 1, autoCommit)(
      config,
      valueDeserializer
    ).head

  /**
    * Consumes the first message available in a given topic, deserializing it as type [[(K, V)]].
    *
    * Only the message that is returned is committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topic        the topic to consume a message from
    * @param autoCommit   if false, only the offset for the consumed message will be committed.
    *                     if true, the offset for the last polled message will be committed instead.
    *                     Defaulted to false.
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param keyDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[K]]
    * @param valueDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[V]]
    * @return the first message consumed from the given topic, with a type [[(K, V)]]
    * @throws TimeoutException          if unable to consume a message within 5 seconds
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[KafkaUnavailableException])
  def consumeFirstKeyedMessageFrom[K, V](
      topic: String,
      autoCommit: Boolean = false
  )(
      implicit config: C,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ): (K, V) =
    consumeNumberKeyedMessagesFrom[K, V](topic, 1, autoCommit)(
      config,
      keyDeserializer,
      valueDeserializer
    ).head

  def consumeNumberMessagesFrom[V](
      topic: String,
      number: Int,
      autoCommit: Boolean = false
  )(implicit config: C, valueDeserializer: Deserializer[V]): List[V] =
    consumeNumberMessagesFromTopics(Set(topic), number, autoCommit)(
      config,
      valueDeserializer
    )(topic)

  def consumeNumberKeyedMessagesFrom[K, V](
      topic: String,
      number: Int,
      autoCommit: Boolean = false
  )(
      implicit config: C,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ): List[(K, V)] =
    consumeNumberKeyedMessagesFromTopics(Set(topic), number, autoCommit)(
      config,
      keyDeserializer,
      valueDeserializer
    )(topic)

  /**
    * Consumes the first n messages available in given topics, deserializes them as type [[V]], and returns
    * the n messages in a Map from topic name to List[V].
    *
    * Only the messages that are returned are committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topics                    the topics to consume messages from
    * @param number                    the number of messages to consume in a batch
    * @param autoCommit                if false, only the offset for the consumed messages will be committed.
    *                                  if true, the offset for the last polled message will be committed instead.
    *                                  Defaulted to false.
    * @param timeout                   the interval to wait for messages before throwing TimeoutException
    * @param resetTimeoutOnEachMessage when true, throw TimeoutException if we have a silent period
    *                                  (no incoming messages) for the timeout interval; when false,
    *                                  throw TimeoutException after the timeout interval if we
    *                                  haven't received all of the expected messages
    * @param config                    an implicit [[EmbeddedKafkaConfig]]
    * @param                           valueDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]]
    *                                  for the type [[V]]
    * @return the List of messages consumed from the given topics, each with a type [[V]]
    * @throws TimeoutException          if unable to consume messages within specified timeout
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  def consumeNumberMessagesFromTopics[V](
      topics: Set[String],
      number: Int,
      autoCommit: Boolean = false,
      timeout: Duration = 5.seconds,
      resetTimeoutOnEachMessage: Boolean = true
  )(
      implicit config: C,
      valueDeserializer: Deserializer[V]
  ): Map[String, List[V]] = {
    consumeNumberKeyedMessagesFromTopics(
      topics,
      number,
      autoCommit,
      timeout,
      resetTimeoutOnEachMessage
    )(config, new StringDeserializer(), valueDeserializer)
      .mapValues(_.map { case (_, m) => m })
  }

  /**
    * Consumes the first n messages available in given topics, deserializes them as type [[(K, V)]], and returns
    * the n messages in a Map from topic name to List[(K, V)].
    *
    * Only the messages that are returned are committed if autoCommit is false.
    * If autoCommit is true then all messages that were polled will be committed.
    *
    * @param topics       the topics to consume messages from
    * @param number       the number of messages to consume in a batch
    * @param autoCommit   if false, only the offset for the consumed messages will be committed.
    *                     if true, the offset for the last polled message will be committed instead.
    *                     Defaulted to false.
    * @param timeout      the interval to wait for messages before throwing TimeoutException
    * @param resetTimeoutOnEachMessage when true, throw TimeoutException if we have a silent period
    *                                  (no incoming messages) for the timeout interval; when false,
    *                                  throw TimeoutException after the timeout interval if we
    *                                  haven't received all of the expected messages
    * @param config       an implicit [[EmbeddedKafkaConfig]]
    * @param keyDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[K]]
    * @param valueDeserializer an implicit [[org.apache.kafka.common.serialization.Deserializer]] for the type [[V]]
    * @return the List of messages consumed from the given topics, each with a type [[(K, V)]]
    * @throws TimeoutException          if unable to consume messages within specified timeout
    * @throws KafkaUnavailableException if unable to connect to Kafka
    */
  def consumeNumberKeyedMessagesFromTopics[K, V](
      topics: Set[String],
      number: Int,
      autoCommit: Boolean = false,
      timeout: Duration = 5.seconds,
      resetTimeoutOnEachMessage: Boolean = true
  )(
      implicit config: C,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V]
  ): Map[String, List[(K, V)]] = {
    val consumerProperties = baseConsumerConfig ++ Map[String, Object](
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> autoCommit.toString
    )

    var timeoutNanoTime = System.nanoTime + timeout.toNanos
    val consumer = new KafkaConsumer[K, V](
      consumerProperties.asJava,
      keyDeserializer,
      valueDeserializer
    )

    val messages = Try {
      val messagesBuffers = topics.map(_ -> ListBuffer.empty[(K, V)]).toMap
      var messagesRead    = 0
      consumer.subscribe(topics.asJava)
      topics.foreach(consumer.partitionsFor)

      while (messagesRead < number && System.nanoTime < timeoutNanoTime) {
        val recordIter =
          consumer.poll(duration2JavaDuration(consumerPollingTimeout)).iterator
        if (resetTimeoutOnEachMessage && recordIter.hasNext) {
          timeoutNanoTime = System.nanoTime + timeout.toNanos
        }
        while (recordIter.hasNext && messagesRead < number) {
          val record = recordIter.next
          messagesBuffers(record.topic) += (record.key -> record.value)
          val tp = new TopicPartition(record.topic, record.partition)
          val om = new OffsetAndMetadata(record.offset + 1)
          consumer.commitSync(Map(tp -> om).asJava)
          messagesRead += 1
        }
      }
      if (messagesRead < number) {
        throw new TimeoutException(
          s"Unable to retrieve $number message(s) from Kafka in $timeout"
        )
      }
      messagesBuffers.mapValues(_.toList)
    }

    consumer.close()
    messages.recover {
      case ex: KafkaException => throw new KafkaUnavailableException(ex)
    }.get
  }
}
