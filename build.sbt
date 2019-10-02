import Merging.customMergeStrategy
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import scoverage.ScoverageKeys._

scalaVersion := "2.12.9"
name := "dxWDL"
organization := "com.dnanexus"

resolvers ++= Seq(
    "Broad Artifactory Releases" at "https://broadinstitute.jfrog.io/broadinstitute/libs-release/"
)

// reduce the maximum number of errors shown by the Scala compiler
maxErrors := 10

//coverageEnabled := true

javacOptions ++= Seq("-Xlint:deprecation")

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
assemblyMergeStrategy in assembly := customMergeStrategy.value

val cromwellV = "46.1"

val googleHttpClientApacheV = "2.1.1"
val googleHttpClientV = "1.29.1"

val googleHttpClientDependencies = List(
    /*
    There is a conflict between versions of com/google/api/client/http/apache/ApacheHttpTransport.class
    which is in both packages. We need these particular versions of the packages.
     */
    "com.google.http-client" % "google-http-client-apache" % googleHttpClientApacheV,
    "com.google.http-client" % "google-http-client" % googleHttpClientV,
    )

libraryDependencies ++= Seq(
    "org.broadinstitute" %% "cromwell-common" % cromwellV,
    "org.broadinstitute" %% "cromwell-core" % cromwellV,
    "org.broadinstitute" %% "cromwell-wom" % cromwellV,
    "org.broadinstitute" %% "cwl-v1-0" % cromwellV,
    "org.broadinstitute" %% "wdl-draft2" % cromwellV,
    "org.broadinstitute" %% "wdl-draft3" % cromwellV,
    "org.broadinstitute" %% "wdl-biscayne" % cromwellV,

    "io.spray" %% "spray-json" % "1.3.5",
    "com.typesafe" % "config" % "1.3.3",

    //---------- Test libraries -------------------//
    "org.scalactic" %% "scalactic" % "3.0.1",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
) ++ googleHttpClientDependencies

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

Test / parallelExecution := false

// comment out this line to enable tests in assembly
test in assembly := {}


// Coverage
//
// sbt clean coverage test
// sbt coverageReport

// To turn it off do:
// sbt coverageOff

// Ignore code parts that cannot be checked in the unit
// test environment
//coverageExcludedPackages := "dxWDL.Main;dxWDL.compiler.DxNI;dxWDL.compiler.DxObjectDirectory;dxWDL.compiler.Native"
