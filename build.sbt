import Merging.customMergeStrategy
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

scalaVersion := "2.13.2"
name := "dxWDL"
organization := "com.dnanexus"
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

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
    "com.dnanexus" %% "wdltools" % "0.1.0-SNAPSHOT",
    // antlr4 lexer + parser
    "org.antlr" % "antlr4" % "4.8",
    // JSON jackson parser
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.0",
    "io.spray" %% "spray-json" % "1.3.5",
    "com.typesafe" % "config" % "1.3.3",
    // http support and libraries used in what remains of dxjava
    "com.google.guava" % "guava" % "18.0",
    "org.apache.httpcomponents" % "httpclient" % "4.5",
    //"org.slf4j" % "slf4j-nop" % "1.7.30",
//  "org.slf4j" % "slf4j-api" % "2.0.0-alpha1",

    //---------- Test libraries -------------------//
    "org.scalactic" % "scalactic_2.13" % "3.1.1",
    "org.scalatest" % "scalatest_2.13" % "3.1.1" % "test"
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
