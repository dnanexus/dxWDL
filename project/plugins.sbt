addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.8")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

// This prevents the SLF dialog from appearing when starting
// an SBT session
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.24"
