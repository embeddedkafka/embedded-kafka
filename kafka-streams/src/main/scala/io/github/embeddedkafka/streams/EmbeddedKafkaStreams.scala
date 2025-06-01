package io.github.embeddedkafka.streams

import java.util.Properties

import io.github.embeddedkafka.ops.{AdminOps, KafkaOps}
import io.github.embeddedkafka.{
  EmbeddedKafka,
  EmbeddedKafkaConfig,
  EmbeddedKafkaSupport,
  UUIDs
}
import org.apache.kafka.streams.{KafkaStreams, Topology}

/**
  * Helper trait for running Kafka Streams. Use `.runStreams` to execute your
  * streams.
  */
trait EmbeddedKafkaStreams
    extends EmbeddedKafkaStreamsSupport[EmbeddedKafkaConfig]
    with EmbeddedKafka {
  override protected[embeddedkafka] val streamsConfig =
    new EmbeddedStreamsConfigImpl
}

private[embeddedkafka] trait EmbeddedKafkaStreamsSupport[
    C <: EmbeddedKafkaConfig
] {
  this: EmbeddedKafkaSupport[C] with AdminOps[C] with KafkaOps =>

  protected[embeddedkafka] def streamsConfig: EmbeddedStreamsConfig[C]

  /**
    * Execute Kafka streams and pass a block of code that can operate while the
    * streams are active. The code block can be used for publishing and
    * consuming messages in Kafka.
    *
    * @param topicsToCreate
    *   the topics that should be created in Kafka before launching the streams.
    * @param topology
    *   the streams topology that will be used to instantiate the streams with a
    *   default configuration (all state directories are different and in temp
    *   folders)
    * @param extraConfig
    *   additional Kafka Streams configuration (overwrite existing keys in
    *   default config)
    * @param block
    *   the code block that will executed while the streams are active. Once the
    *   block has been executed the streams will be closed.
    */
  def runStreams[T](
      topicsToCreate: Seq[String],
      topology: Topology,
      extraConfig: Map[String, AnyRef] = Map.empty
  )(block: => T)(implicit config: C): T =
    runStreamsOnFoundPort(config)(topicsToCreate, topology, extraConfig)(_ =>
      block
    )

  /**
    * Execute Kafka streams and pass a block of code that can operate while the
    * streams are active. The code block can be used for publishing and
    * consuming messages in Kafka. The actual ports of the servers will be
    * detected and inserted into a copied version of the [[EmbeddedKafkaConfig]]
    * that gets passed to body. This is useful if you set any port to `0`, which
    * will listen on an arbitrary available port.
    *
    * @param config
    *   the user-defined [[EmbeddedKafkaConfig]]
    * @param topicsToCreate
    *   the topics that should be created in Kafka before launching the streams.
    * @param topology
    *   the streams topology that will be used to instantiate the streams with a
    *   default configuration (all state directories are different and in temp
    *   folders)
    * @param extraConfig
    *   additional Kafka Streams configuration (overwrite existing keys in
    *   default config)
    * @param block
    *   the code block that will executed while the streams are active, given an
    *   [[EmbeddedKafkaConfig]] with the actual ports the servers are running
    *   on. Once the block has been executed the streams will be closed.
    */
  def runStreamsOnFoundPort[T](config: C)(
      topicsToCreate: Seq[String],
      topology: Topology,
      extraConfig: Map[String, AnyRef] = Map.empty
  )(block: C => T): T =
    withRunningKafkaOnFoundPort(config) { implicit configWithUsedPorts =>
      topicsToCreate.foreach(topic => createCustomTopic(topic))
      val streamId = UUIDs.newUuid().toString
      val streams  = new KafkaStreams(
        topology,
        map2Properties(streamsConfig.config(streamId, extraConfig))
      )
      streams.start()
      try {
        block(configWithUsedPorts)
      } finally {
        streams.close()
      }
    }

  private def map2Properties(map: Map[String, AnyRef]): Properties = {
    val props = new Properties

    map.foreach {
      case (k, v) => props.put(k, v)
    }

    props
  }
}

object EmbeddedKafkaStreams extends EmbeddedKafkaStreams
