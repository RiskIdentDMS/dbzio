import ReleaseTransformations._
import Build._

ThisBuild / versionScheme := Some("early-semver")
ThisBuild / organization := "com.riskident"
ThisBuild / organizationName := "Risk.Ident GmbH"
ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"

Global / credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
Global / publishTo := {
  val nexus = "https://nexus3.2rioffice.com/repository/dbzio/"
  Some("Frida snapshot repository" at nexus + (if (isSnapshot.value) "snapshots" else "releases"))
}

lazy val dbzio = (project in file("dbzio")).withScalafix.withCommonSettings
  .settings(
    name := "dbzio"
  )

lazy val test = (project in file("test")).withScalafix.withCommonSettings
  .dependsOn(dbzio)
  .settings(
    name := "dbzio-test",
    libraryDependencies ++= Seq(
      "com.h2database"     % "h2"              % Version.h2,
      "com.typesafe.slick" %% "slick-hikaricp" % Version.slick,
      "dev.zio"            %% "zio-test"       % Version.zio
    )
  )

lazy val tests = (project in file("tests"))
  .dependsOn(dbzio, test)
  .withScalafix
  .withCommonSettings
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "com.h2database"             % "h2"                         % Version.h2             % Test,
      "org.slf4j"                  % "slf4j-nop"                  % Version.slf4j          % Test,
      "com.typesafe.slick"         %% "slick-hikaricp"            % Version.slick          % Test,
      "dev.zio"                    %% "zio-test"                  % Version.zio            % Test,
      "dev.zio"                    %% "zio-test-sbt"              % Version.zio            % Test,
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % Version.shapelessCheck % Test,
      "org.scalacheck"             %% "scalacheck"                % Version.scalaCheck     % Test,
      "org.typelevel"              %% "cats-core"                 % Version.cats           % Test,
      "org.typelevel"              %% "cats-laws"                 % Version.cats           % Test
    )
  )

lazy val root = (project in file("."))
  .aggregate(dbzio, test, tests)
  .withScalafix
  .settings(
    publish / skip := true,
    crossScalaVersions := supportedScalaVersions,
    /**
      * release settings
      */
    publishMavenStyle := true,
    // Workaround from https://www.scala-sbt.org/1.x/docs/Cross-Build.html#Note+about+sbt-release
    // don't use sbt-release's cross facility
    releaseCrossBuild := false,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      releaseStepCommandAndRemaining("+test"),
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommandAndRemaining("+publish"),
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )

addCommandAlias(
  "fmt",
  """;eval println("Formatting source code");scalafmt;eval println("Formatting test code");Test / scalafmt;eval println("Formatting SBT files");scalafmtSbt"""
)
