addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.8")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.2.1")

// This prevents the SLF dialog from appearing when starting
// an SBT session
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.24"
// only load git plugin if we're actually in a git repo
libraryDependencies ++= {
  if (baseDirectory.value / "../.git" isDirectory)
    Seq(
        Defaults.sbtPluginExtra("com.typesafe.sbt" % "sbt-git" % "1.0.0",
                                (sbtBinaryVersion in update).value,
                                (scalaBinaryVersion in update).value)
    )
  else {
    println("sbt-git plugin not loaded")
    Seq.empty
  }
}
