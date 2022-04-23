import ReleaseTransformations._

lazy val Version = new {
  lazy val scala213 = "2.13.8"
  lazy val scala212 = "2.12.15"

  val h2             = "2.1.210"
  val slf4j          = "1.7.36"
  val zio            = "1.0.13"
  val cats           = "2.7.0"
  val scalaCheck     = "1.15.4"
  val slick          = "3.3.3"
  val shapeless      = "2.3.8"
  val shapelessCheck = "1.3.0"
}

lazy val supportedScalaVersions = List(Version.scala213, Version.scala212)

ThisBuild / versionScheme := Some("early-semver")
ThisBuild / organization := "com.riskident"
ThisBuild / organizationName := "Risk.Ident GmbH"

Global / credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
Global / publishTo := {
  val nexus = "https://nexus3.2rioffice.com/repository/dbzio/"
  Some("Frida snapshot repository" at nexus + (if (isSnapshot.value) "snapshots" else "releases"))
}

def createScalacOptions(version: String, unusedImport: Boolean): List[String] = {
  val base = List(
    "-explaintypes",
    "-feature",
    "-Xlint",
    "-unchecked",
    "-encoding",
    "UTF-8",
    "-deprecation",
    "-language:higherKinds",
    "-opt-inline-from:com.riskident.**",
    "-opt:l:method,inline",
    "-opt-warnings:none"
  )

  val wConf = List(
    "-Ywarn-macros:after",
    "-Wconf:" + List(
      "cat=deprecation:ws",
      "cat=feature:ws",
      "cat=unused-params:s",
      "cat=unused-pat-vars:e",
      "cat=unused-privates:s",
      "cat=unused-locals:s",
      "cat=unused-nowarn:s",
      "src=src_managed/.*:s",
      s"cat=unused-imports:${if (unusedImport) "e" else "s"}"
    ).mkString(",")
  )

  CrossVersion.partialVersion(version) match {
    case Some((2, 12)) => (base :+ "-Ypartial-unification") ++ wConf
    case Some((2, 13)) => base ++ wConf
    case _             => base ++ wConf
  }
}
lazy val commonSettings = Seq(
  crossScalaVersions := supportedScalaVersions,
  addCompilerPlugin("org.scalameta" % "semanticdb-scalac" % "4.5.4" cross CrossVersion.full),
  scalacOptions += "-Yrangepos",
  libraryDependencies ++= Seq(
    "com.typesafe.slick" %% "slick"        % Version.slick,
    "com.chuusai"        %% "shapeless"    % Version.shapeless,
    "dev.zio"            %% "zio"          % Version.zio,
    "org.scala-lang"     % "scala-reflect" % scalaVersion.value
  ),
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  Test / fork := true,
  scalacOptions := createScalacOptions(scalaVersion.value, true),
  Compile / console / scalacOptions := createScalacOptions(scalaVersion.value, false),
  Test / console / scalacOptions := (Compile / console / scalacOptions).value
)
lazy val dbzio = (project in file("dbzio"))
  .settings(commonSettings: _*)
  .settings(
    name := "dbzio"
  )

lazy val test = (project in file("test"))
  .dependsOn(dbzio)
  .settings(commonSettings: _*)
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
  .settings(commonSettings: _*)
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
  .settings(
    publish / skip := true,
    crossScalaVersions := supportedScalaVersions,
    // Workaround from https://www.scala-sbt.org/1.x/docs/Cross-Build.html#Note+about+sbt-release
    // don't use sbt-release's cross facility
    releaseCrossBuild := false,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
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
