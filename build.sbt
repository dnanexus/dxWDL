//import com.typesafe.sbt.GitPlugin.autoImport._
import sbt.Keys._
enablePlugins(GitVersioning)

//scalaVersion := "2.11.8"
scalaVersion := "2.11.6"

name := "dxWDL"
organization := "com.dnanexus"

// Shorten the git commit hash
git.gitHeadCommit := git.gitHeadCommit.value map { _.take(8) }
versionWithGit
assemblyJarName in assembly := "dxWDL-" + git.gitHeadCommit.value + ".jar"

logLevel in assembly := Level.Info

scalacOptions ++= Seq("-unchecked", "-deprecation")
resolvers ++= Seq(
  "Broad Artifactory Releases" at "https://artifactory.broadinstitute.org/artifactory/libs-release/",
  "Broad Artifactory Snapshots" at "https://artifactory.broadinstitute.org/artifactory/libs-snapshot/"
)

// Exclude the dnanexus java bindings in the assembled fat JAR file.
// We have access to the dnanexus jar file at runtime on the cloud instance.
assemblyExcludedJars in assembly := {
    val cp = (fullClasspath in assembly).value
    cp filter {_.data.getName contains "dnanexus-api"}
}

libraryDependencies ++= Seq(
    "org.broadinstitute" %% "wdl4s" % "0.9",
    "com.google.code.findbugs" % "jsr305" % "1.3.+",

    //---------- Test libraries -------------------//
    "org.scalatest" %% "scalatest" % "2.2.5" % Test,
    "net.jcazevedo" %% "moultingyaml" % "0.3.0"
)
