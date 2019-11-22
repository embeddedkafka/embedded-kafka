addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.12")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.0")

// TODO: revert. brings in sbt-sonatype which conflicts with sbt-bintray
//addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.4.31")

// TODO: remove
addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.5")
