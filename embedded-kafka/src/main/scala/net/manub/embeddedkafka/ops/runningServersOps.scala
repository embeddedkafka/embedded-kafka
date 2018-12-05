package net.manub.embeddedkafka.ops

import net.manub.embeddedkafka.{EmbeddedKafkaConfig, EmbeddedServer}

object RunningServersOps {

  /**
    * Wrapper class providing methods for keeping track
    * of running [[EmbeddedServer]]s.
    */
  class RunningServers {

    private[this] var servers: Seq[EmbeddedServer] = Seq.empty

    /**
      * Returns the list of running [[EmbeddedServer]]s.
      * @return
      */
    def list: List[EmbeddedServer] = servers.toList

    /**
      * Adds a running [[EmbeddedServer]] to the list.
      * @param s an [[EmbeddedServer]]
      * @return
      */
    def add(s: EmbeddedServer): this.type = {
      servers :+= s
      this
    }

    /**
      * Stops and removes each [[EmbeddedServer]] matching the provided
      * predicate from the list of running [[EmbeddedServer]]s.
      *
      * @param removalPredicate the predicate for removing [[EmbeddedServer]]s
      * @param clearLogs        whether or not to clear server logs, if any.
      * @return
      */
    def stopAndRemove(removalPredicate: EmbeddedServer => Boolean,
                      clearLogs: Boolean = true): this.type = {
      servers = servers.flatMap {
        case s if removalPredicate(s) =>
          s.stop(clearLogs)
          None
        case s => Option(s)
      }
      this
    }

  }

}

/**
  * Trait for keeping track of running [[EmbeddedServer]]s.
  */
trait RunningServersOps {
  import RunningServersOps._

  private[embeddedkafka] val runningServers = new RunningServers

  /**
    * Returns whether the in memory servers are running.
    */
  def isRunning: Boolean

  /**
    * Stops all in memory servers and deletes the log directories.
    */
  def stop(): Unit = runningServers.stopAndRemove(_ => true)

  /**
    * Stops a specific [[EmbeddedServer]] instance, and deletes the log directory.
    *
    * @param server the [[EmbeddedServer]] to be stopped.
    */
  def stop(server: EmbeddedServer): Unit =
    runningServers.stopAndRemove(_ == server)

}

/**
  * Trait for starting [[EmbeddedServer]] instances.
  * Relies on [[RunningServersOps]] for keeping track of running [[EmbeddedServer]]s.
  *
  * @tparam C an [[EmbeddedKafkaConfig]]
  * @tparam S an [[EmbeddedServer]]
  *
  * @see [[RunningServersOps]]
  */
trait ServerStarter[C <: EmbeddedKafkaConfig, S <: EmbeddedServer] {
  this: RunningServersOps =>

  /**
    * Starts in memory servers, using temporary directories for storing logs.
    * The log directories will be cleaned after calling the [[EmbeddedServer.stop()]] method or on JVM exit, whichever happens earlier.
    *
    * @param config an implicit [[EmbeddedKafkaConfig]]
    */
  def start()(implicit config: C): S

}
