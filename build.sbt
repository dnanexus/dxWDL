import Merging.customMergeStrategy
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

scalaVersion := "2.13.2"
name := "dxWDL"
import com.typesafe.config._
val confPath =
  Option(System.getProperty("config.file")).getOrElse("src/main/resources/application.conf")
val conf = ConfigFactory.parseFile(new File(confPath)).resolve()
version := conf.getString("dxWDL.version")
organization := "com.dnanexus"
developers := List(
    Developer("orodeh", "orodeh", "orodeh@dnanexus.com", url("https://github.com/dnanexus")),
    Developer("jdidion", "jdidion", "jdidion@dnanexus.com", url("https://github.com/dnanexus")),
    Developer("xquek", "xquek", "xquek@dnanexus.com", url("https://github.com/dnanexus")),
    Developer("commandlinegirl",
              "commandlinegirl",
              "azalcman@dnanexus.com",
              url("https://github.com/dnanexus")),
    Developer("mhrvol", "mhrvol", "mhrvol-cf@dnanexus.com", url("https://github.com/dnanexus"))
)
homepage := Some(url("https://github.com/dnanexus/dxWDL"))
scmInfo := Some(
    ScmInfo(url("https://github.com/dnanexus/dxWDL"), "git@github.com:dnanexus/dxWDL.git")
)
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

val root = project.in(file("."))

// reduce the maximum number of errors shown by the Scala compiler
maxErrors := 7

//coverageEnabled := true

javacOptions ++= Seq("-Xlint:deprecation")

// Show deprecation warnings
scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-explaintypes",
    "-encoding",
    "UTF-8",
    "-Xlint:constant",
    "-Xlint:delayedinit-select",
    "-Xlint:doc-detached",
    "-Xlint:inaccessible",
    "-Xlint:infer-any",
    "-Xlint:nullary-override",
    "-Xlint:nullary-unit",
    "-Xlint:option-implicit",
    "-Xlint:package-object-classes",
    "-Xlint:poly-implicit-overload",
    "-Xlint:private-shadow",
    "-Xlint:stars-align",
    "-Xlint:type-parameter-shadow",
    "-Ywarn-dead-code",
    "-Ywarn-unused:implicits",
    "-Ywarn-unused:privates",
    "-Ywarn-unused:locals",
    "-Ywarn-unused:imports", // warns about every unused import on every command.
    "-Xfatal-warnings" // makes those warnings fatal.
)

assemblyJarName in assembly := "dxWDL.jar"
logLevel in assembly := Level.Info
assemblyOutputPath in assembly := file("applet_resources/resources/dxWDL.jar")
assemblyMergeStrategy in assembly := customMergeStrategy.value

val wdlToolsVersion = "0.5.1"
val typesafeVersion = "1.3.3"
val sprayVersion = "1.3.5"
val jacksonVersion = "2.11.0"
val guavaVersion = "18.0"
val httpClientVersion = "4.5"
//val scalacticVersion = "3.1.1"
val scalatestVersion = "3.1.1"

libraryDependencies ++= Seq(
    "com.dnanexus" % "wdltools" % wdlToolsVersion,
    "io.spray" %% "spray-json" % sprayVersion,
    "com.typesafe" % "config" % typesafeVersion,
    // libraries used in what remains of dxjava
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.google.guava" % "guava" % guavaVersion,
    "org.apache.httpcomponents" % "httpclient" % httpClientVersion,
    //---------- Test libraries -------------------//
    //"org.scalactic" % "scalactic_2.13" % scalacticVersion,
    "org.scalatest" % "scalatest_2.13" % scalatestVersion % "test"
)

// If an exception is thrown during tests, show the full
// stack trace, by adding the "-oF" option to the list.
//

// exclude the native tests, they are slow.
// to do this from the command line:
// sbt testOnly -- -l native
//
// comment out this line in order to allow native
// tests
// Test / testOptions += Tests.Argument("-l", "native")
Test / testOptions += Tests.Argument("-oF")

Test / parallelExecution := false

// comment out this line to enable tests in assembly
test in assembly := {}

// scalafmt
scalafmtConfig := root.base / ".scalafmt.conf"
// Coverage
//
// sbt clean coverage test
// sbt coverageReport

// To turn it off do:
// sbt coverageOff

// Ignore code parts that cannot be checked in the unit
// test environment
//coverageExcludedPackages := "dxWDL.Main;dxWDL.compiler.DxNI;dxWDL.compiler.DxObjectDirectory;dxWDL.compiler.Native"
