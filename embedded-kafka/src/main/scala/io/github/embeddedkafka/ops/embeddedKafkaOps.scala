package io.github.embeddedkafka.ops

import io.github.embeddedkafka.{EmbeddedKafkaConfig, EmbeddedServer}

/**
  * Combined trait with all Ops traits.
  *
  * @tparam C
  *   an [[EmbeddedKafkaConfig]]
  * @tparam S
  *   an `EmbeddedServer`
  */
private[embeddedkafka] trait EmbeddedKafkaOps[
    C <: EmbeddedKafkaConfig,
    S <: EmbeddedServer
] extends AdminOps[C]
    with ConsumerOps[C]
    with ProducerOps[C]
    with ZooKeeperOps
    with KafkaOps

/**
  * [[EmbeddedKafkaOps]] extension relying on `RunningServersOps` for keeping
  * track of running `EmbeddedServer`s.
  *
  * @tparam C
  *   an [[EmbeddedKafkaConfig]]
  * @tparam S
  *   an `EmbeddedServer`
  */
private[embeddedkafka] trait RunningEmbeddedKafkaOps[
    C <: EmbeddedKafkaConfig,
    S <: EmbeddedServer
] extends EmbeddedKafkaOps[C, S]
    with RunningServersOps
    with ServerStarter[C, S]
    with RunningZooKeeperOps
    with RunningKafkaOps
