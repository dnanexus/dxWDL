//import com.typesafe.sbt.GitPlugin.autoImport._
import sbt.Keys._
enablePlugins(GitVersioning)

scalaVersion := "2.12.1"
name := "dxWDL"
organization := "com.dnanexus"

// Shorten the git commit hash
git.gitHeadCommit := git.gitHeadCommit.value map { _.take(8) }
versionWithGit

resolvers ++= Seq(
  "Broad Artifactory Releases" at "https://artifactory.broadinstitute.org/artifactory/libs-release/",
  "Broad Artifactory Snapshots" at "https://artifactory.broadinstitute.org/artifactory/libs-snapshot/"
)

// Show deprecation warnings
scalacOptions ++= Seq("-unchecked", "-deprecation")

// Assembly settings
assemblyJarName in assembly := "dxWDL.jar"
logLevel in assembly := Level.Info

libraryDependencies ++= Seq(
    "org.broadinstitute" %% "wdl4s" % "0.12",
    "com.google.code.findbugs" % "jsr305" % "1.3.+",
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
