package io.github.embeddedkafka.streams

import java.nio.file.Files

import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}
import org.apache.kafka.streams.StreamsConfig

/**
  * Kafka Streams config generator.
  *
  * @tparam C
  *   an [[EmbeddedKafkaConfig]]
  */
private[embeddedkafka] trait EmbeddedStreamsConfig[C <: EmbeddedKafkaConfig] {
  protected[embeddedkafka] def baseStreamConfig(streamName: String)(
      implicit kafkaConfig: C
  ): Map[String, AnyRef] =
    Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> streamName,
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:${kafkaConfig.kafkaPort}",
      StreamsConfig.STATE_DIR_CONFIG -> Files
        .createTempDirectory(streamName)
        .toString,
      // force stream consumers to start reading from the beginning so as not to lose messages
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OffsetResetStrategy.EARLIEST.toString.toLowerCase
    )

  /**
    * @param streamName
    *   the name of the stream. It will be used as the Application ID
    * @param extraConfig
    *   any additional configuration. If the keys are already defined in the
    *   default they will be overwritten with this
    * @param kafkaConfig
    *   the Kafka test configuration
    * @return
    *   a map of config parameters for running Kafka Streams
    */
  def config(streamName: String, extraConfig: Map[String, AnyRef])(
      implicit kafkaConfig: C
  ): Map[String, AnyRef] =
    baseStreamConfig(streamName) ++ extraConfig
}

final class EmbeddedStreamsConfigImpl
    extends EmbeddedStreamsConfig[EmbeddedKafkaConfig]
