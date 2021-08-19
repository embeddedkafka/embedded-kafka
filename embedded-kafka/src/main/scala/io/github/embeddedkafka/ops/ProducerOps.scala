package io.github.embeddedkafka.ops

import io.github.embeddedkafka.{
  EmbeddedKafkaConfig,
  KafkaUnavailableException,
  loanAndClose
}
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

/**
  * Trait for Producer-related actions.
  *
  * @tparam C
  *   an [[EmbeddedKafkaConfig]]
  */
trait ProducerOps[C <: EmbeddedKafkaConfig] {
  protected val producerPublishTimeout: FiniteDuration = 10.seconds

  private[embeddedkafka] def baseProducerConfig(
      implicit config: C
  ): Map[String, Object]

  private[embeddedkafka] def defaultProducerConf(implicit config: C) =
    Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${config.kafkaPort}",
      ProducerConfig.MAX_BLOCK_MS_CONFIG     -> 10000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
    )

  /**
    * Publishes synchronously a message of type `String` to the running Kafka
    * broker.
    *
    * @param topic
    *   the topic to which publish the message (it will be auto-created)
    * @param message
    *   the message to publish
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @throws KafkaUnavailableException
    *   if unable to connect to Kafka
    */
  def publishStringMessageToKafka(topic: String, message: String)(
      implicit config: C
  ): Unit =
    publishToKafka(topic, message)(config, new StringSerializer)

  /**
    * Publishes synchronously a message to the running Kafka broker.
    *
    * @param topic
    *   the topic to which publish the message (it will be auto-created)
    * @param message
    *   the message of type `T` to publish
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @param serializer
    *   an implicit `Serializer` for the type `T`
    * @throws KafkaUnavailableException
    *   if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[T](
      topic: String,
      message: T
  )(implicit config: C, serializer: Serializer[T]): Unit =
    publishToKafka(
      new KafkaProducer(
        baseProducerConfig.asJava,
        new StringSerializer(),
        serializer
      ),
      new ProducerRecord[String, T](topic, message)
    )

  /**
    * Publishes synchronously a message to the running Kafka broker.
    *
    * @param producerRecord
    *   the producerRecord of type `T` to publish
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @param serializer
    *   an implicit `Serializer` for the type `T`
    * @throws KafkaUnavailableException
    *   if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[T](
      producerRecord: ProducerRecord[String, T]
  )(implicit config: C, serializer: Serializer[T]): Unit =
    publishToKafka(
      new KafkaProducer(
        baseProducerConfig.asJava,
        new StringSerializer(),
        serializer
      ),
      producerRecord
    )

  /**
    * Publishes synchronously a message to the running Kafka broker.
    *
    * @param topic
    *   the topic to which publish the message (it will be auto-created)
    * @param key
    *   the key of type `K` to publish
    * @param message
    *   the message of type `T` to publish
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @param serializer
    *   an implicit `Serializer` for the type `T`
    * @throws KafkaUnavailableException
    *   if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[K, T](topic: String, key: K, message: T)(
      implicit config: C,
      keySerializer: Serializer[K],
      serializer: Serializer[T]
  ): Unit =
    publishToKafka(
      new KafkaProducer(baseProducerConfig.asJava, keySerializer, serializer),
      new ProducerRecord(topic, key, message)
    )

  /**
    * Publishes synchronously a batch of message to the running Kafka broker.
    *
    * @param topic
    *   the topic to which publish the message (it will be auto-created)
    * @param messages
    *   the keys and messages of type `(K, T)` to publish
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @param keySerializer
    *   an implicit `Serializer` for the type `K`
    * @param serializer
    *   an implicit `Serializer` for the type `T`
    * @throws KafkaUnavailableException
    *   if unable to connect to Kafka
    */
  @throws(classOf[KafkaUnavailableException])
  def publishToKafka[K, T](topic: String, messages: Seq[(K, T)])(
      implicit config: C,
      keySerializer: Serializer[K],
      serializer: Serializer[T]
  ): Unit = {
    val producer =
      new KafkaProducer(baseProducerConfig.asJava, keySerializer, serializer)

    val tupleToRecord = (new ProducerRecord(topic, _: K, _: T)).tupled

    val futureSend = tupleToRecord andThen producer.send

    val futures = messages.map(futureSend)

    // Assure all messages sent before returning, and fail on first send error
    val records = futures.map(f =>
      Try(f.get(producerPublishTimeout.length, producerPublishTimeout.unit))
    )

    producer.close()

    val _ = records.collectFirst {
      case Failure(ex) => throw new KafkaUnavailableException(ex)
    }
  }

  /**
    * Loaner pattern that allows running a code block with a newly created
    * consumer. The consumer's lifecycle will be automatically handled and
    * closed at the end of the given code block.
    *
    * @param config
    *   an implicit [[EmbeddedKafkaConfig]]
    * @param keySerializer
    *   an implicit `Serializer` for the type `K`
    * @param valueSerializer
    *   an implicit `Serializer` for the type `V`
    * @param body
    *   the function to execute that returns `T`
    */
  def withProducer[K, V, T](body: KafkaProducer[K, V] => T)(
      implicit config: C,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V]
  ): T =
    loanAndClose(
      new KafkaProducer(
        baseProducerConfig.asJava,
        keySerializer,
        valueSerializer
      )
    )(body)

  private def publishToKafka[K, T](
      kafkaProducer: KafkaProducer[K, T],
      record: ProducerRecord[K, T]
  ): Unit = {
    val sendFuture = kafkaProducer.send(record)
    val sendResult = Try {
      sendFuture.get(producerPublishTimeout.length, producerPublishTimeout.unit)
    }

    kafkaProducer.close()

    sendResult match {
      case Failure(ex) => throw new KafkaUnavailableException(ex)
      case _           => // OK
    }
  }

}
