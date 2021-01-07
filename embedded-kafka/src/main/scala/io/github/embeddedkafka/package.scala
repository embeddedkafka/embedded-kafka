package io.github

import scala.concurrent.duration.FiniteDuration

package object embeddedkafka {
  implicit private[embeddedkafka] class ServerOps(
      servers: Seq[EmbeddedServer]
  ) {
    def toFilteredSeq[T <: EmbeddedServer](
        filter: EmbeddedServer => Boolean
    ): Seq[T] =
      servers.filter(filter).asInstanceOf[Seq[T]]
  }

  def duration2JavaDuration(d: FiniteDuration): java.time.Duration =
    java.time.Duration.ofNanos(d.toNanos)

  def loanAndClose[A <: AutoCloseable, B](a: A)(f: A => B): B = {
    try {
      f(a)
    } finally {
      a.close()
    }
  }
}
