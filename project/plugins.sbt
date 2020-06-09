addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.8")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.2.1")

// This prevents the SLF dialog from appearing when starting
// an SBT session
//libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.30"
//libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.0-alpha1"
//
// workaround for missing static SLF4J binder for logback
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
