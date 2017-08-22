//import com.typesafe.sbt.GitPlugin.autoImport._
import sbt.Keys._
enablePlugins(GitVersioning)

scalaVersion := "2.12.3"
name := "dxWDL"
organization := "com.dnanexus"

// Shorten the git commit hash
git.gitHeadCommit := git.gitHeadCommit.value map { _.take(8) }
versionWithGit

resolvers ++= Seq(
    "Broad Artifactory Releases" at "https://broadinstitute.jfrog.io/broadinstitute/libs-release/"
)

// Show deprecation warnings
scalacOptions ++= Seq("-unchecked", "-deprecation")

// Assembly settings
assemblyJarName in assembly := "dxWDL.jar"
logLevel in assembly := Level.Info
assemblyOutputPath in assembly := file("applet_resources/resources/dxWDL.jar")

libraryDependencies ++= Seq(
    "org.broadinstitute" %% "wdl4s-wdl" % "0.15",
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
