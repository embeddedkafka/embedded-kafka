package net.manub

import scala.concurrent.duration.FiniteDuration

package object embeddedkafka {

  implicit class ServerOps(servers: Seq[EmbeddedServer]) {
    def toFilteredSeq[T <: EmbeddedServer](
        filter: EmbeddedServer => Boolean): Seq[T] =
      servers.filter(filter).asInstanceOf[Seq[T]]
  }

  def duration2JavaDuration(d: FiniteDuration): java.time.Duration =
    java.time.Duration.ofNanos(d.toNanos)

}
