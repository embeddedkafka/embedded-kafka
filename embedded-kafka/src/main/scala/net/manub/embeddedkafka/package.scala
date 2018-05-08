package net.manub

package object embeddedkafka {

  implicit class ServerOps(servers: Seq[EmbeddedServer]) {
    def toFilteredSeq[T <: EmbeddedServer](
        filter: EmbeddedServer => Boolean): Seq[T] =
      servers.filter(filter).asInstanceOf[Seq[T]]
  }
}
