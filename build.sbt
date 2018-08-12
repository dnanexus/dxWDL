import Merging.customMergeStrategy
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import scoverage.ScoverageKeys._

scalaVersion := "2.12.6"
name := "dxWDL"
organization := "com.dnanexus"

resolvers ++= Seq(
    "Broad Artifactory Releases" at "https://broadinstitute.jfrog.io/broadinstitute/libs-release/"
)

// reduce the maximum number of errors shown by the Scala compiler
maxErrors := 10

//coverageEnabled := true

// Show deprecation warnings
scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-explaintypes",
    "-encoding", "UTF-8",

    "-Xfuture",
    "-Xlint:by-name-right-associative",
    "-Xlint:constant",
    "-Xlint:delayedinit-select",
    "-Xlint:doc-detached",
    "-Xlint:inaccessible",
    "-Xlint:infer-any",
//    "-Xlint:missing-interpolator",
    "-Xlint:nullary-override",
    "-Xlint:nullary-unit",
    "-Xlint:option-implicit",
    "-Xlint:package-object-classes",
    "-Xlint:poly-implicit-overload",
    "-Xlint:private-shadow",
    "-Xlint:stars-align",
    "-Xlint:type-parameter-shadow",
    "-Ypartial-unification",  // https://typelevel.org/cats
    "-Ywarn-dead-code",
    "-Ywarn-inaccessible",
    "-Ywarn-unused:implicits",
    "-Ywarn-unused:privates",
    "-Ywarn-unused:locals",
    "-Ywarn-unused:imports", // warns about every unused import on every command.
    "-Xfatal-warnings"       // makes those warnings fatal.
)

assemblyJarName in assembly := "dxWDL.jar"
logLevel in assembly := Level.Info
assemblyOutputPath in assembly := file("applet_resources/resources/dxWDL.jar")
assemblyMergeStrategy in assembly := customMergeStrategy

val cromwellV = "35-fb4e1ec-SNAP"

libraryDependencies ++= Seq(
    "org.broadinstitute" %% "cromwell-common" % cromwellV,
    "org.broadinstitute" %% "cromwell-core" % cromwellV,
    "org.broadinstitute" %% "cromwell-wom" % cromwellV,
    "org.broadinstitute" %% "cwl-v1-0" % cromwellV,
    "org.broadinstitute" %% "language-factory-core" % cromwellV,
    "org.broadinstitute" %% "wdl-draft2" % cromwellV,
    "org.broadinstitute" %% "wdl-draft3" % cromwellV,
    "org.broadinstitute" %% "wdl-biscayne" % cromwellV,

    // "cromwell.languages.util"
    "io.spray" %% "spray-json" % "1.3.2",
    "net.jcazevedo" %% "moultingyaml" % "0.4.0",
    "com.typesafe" % "config" % "1.3.1",

    //---------- Test libraries -------------------//
    "org.scalactic" %% "scalactic" % "3.0.1",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

// If an exception is thrown during tests, show the full
// stack trace
//testOptions in Test += Tests.Argument("-oF")
