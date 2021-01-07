package io.github.embeddedkafka.connect

import java.nio.file.Path

import io.github.embeddedkafka.EmbeddedKafkaConfig

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig
import org.apache.kafka.connect.runtime.WorkerConfig

/**
  * Kafka Connect config generator.
  *
  * @tparam C an [[EmbeddedKafkaConfig]]
  */
private[embeddedkafka] trait EmbeddedConnectConfig[C <: EmbeddedKafkaConfig] {
  protected[embeddedkafka] def baseConnectConfig(
      connectPort: Int,
      offsets: Path
  )(
      implicit kafkaConfig: C
  ): Map[String, String] =
    Map(
      WorkerConfig.LISTENERS_CONFIG                        -> s"http://localhost:$connectPort",
      WorkerConfig.BOOTSTRAP_SERVERS_CONFIG                -> s"localhost:${kafkaConfig.kafkaPort}",
      WorkerConfig.KEY_CONVERTER_CLASS_CONFIG              -> "org.apache.kafka.connect.json.JsonConverter",
      WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG            -> "org.apache.kafka.connect.json.JsonConverter",
      WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG        -> "10000",
      StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG -> offsets.toAbsolutePath.toString
    )

  /**
    * @param connectPort the Kafka Connect port for the REST API to listen on.
    * @param offsets     the file to store offset data in.
    * @param extraConfig any additional configuration. If the keys are already defined
    *                    in the default they will be overwritten with this.
    * @param kafkaConfig the Kafka test configuration.
    * @return a map of config parameters for running the Kafka Connect server.
    */
  def config(connectPort: Int, offsets: Path, extraConfig: Map[String, String])(
      implicit kafkaConfig: C
  ): Map[String, String] =
    baseConnectConfig(connectPort, offsets) ++ extraConfig
}

final class EmbeddedConnectConfigImpl
    extends EmbeddedConnectConfig[EmbeddedKafkaConfig]
